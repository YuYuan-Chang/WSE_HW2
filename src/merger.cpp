// merger.cpp
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <queue>
#include <memory>
#include <algorithm>
#include <cstdint>
#include <sstream>
#include <chrono>

namespace fs = std::filesystem;
using namespace std;

const int POSTING_PER_BLOCK = 64;

// Struct definitions
struct Posting {
    int docID;
    int termFreq;
};

struct LexiconEntry {
    string term;
    uint64_t offset;     // Byte offset in the final index file
    uint32_t length;     // Number of bytes for this term's entry
    uint32_t docFreq;    // Number of documents containing the term
};

struct BlockMetaData {
    uint32_t size;
    int lastDocID;
};

// PostingFileReader class to handle reading from intermediate text files
class PostingFileReader {
public:
    PostingFileReader(const string& filepath) : infile(filepath), eof(false) {
        if (!infile.is_open()) {
            throw runtime_error("Failed to open intermediate file: " + filepath);
        }
        readNextTerm();
    }

    bool hasNext() const {
        return !eof;
    }

    const string& getCurrentTerm() const {
        return currentTerm;
    }

    const vector<Posting>& getCurrentPostings() const {
        return currentPostings;
    }

    void readNextTerm() {
        if (!getline(infile, currentLine)) {
            eof = true;
            return;
        }

        // Parse the current line
        istringstream iss(currentLine);
        iss >> currentTerm;

        currentPostings.clear();
        string postingStr;
        while (iss >> postingStr) {

            size_t colonPos = postingStr.find(':');
            if (colonPos == string::npos) {
                throw runtime_error("Malformed posting: " + postingStr);
            }

            int docID = stoi(postingStr.substr(0, colonPos));
            int termFreq = stoi(postingStr.substr(colonPos + 1));

            // Validate docID and termFreq
            if (docID < 0 || termFreq < 0) {
                throw runtime_error("Invalid docID or termFreq in posting: " + postingStr);
            }

            currentPostings.emplace_back(Posting{docID, termFreq});
        }
    }

private:
    ifstream infile;
    bool eof;
    string currentLine;
    string currentTerm;
    vector<Posting> currentPostings;
};

// Comparator for the priority queue (min heap based on term lexicographical order)
struct ComparePQNode {
    bool operator()(const pair<string, size_t>& a, const pair<string, size_t>& b) {
        // Min heap based on term, and then based on the file_index
        if (a.first == b.first) {
            return a.second > b.second;
        }
        return a.first > b.first; 
    }
};

// Function to list intermediate text files
vector<string> listIntermediateFiles(const string& directory) {
    vector<string> files;
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (entry.is_regular_file() && entry.path().extension() == ".txt") {
            files.push_back(entry.path().string());
        }
    }
    // Sort files to ensure consistent merging order
    sort(files.begin(), files.end());
    return files;
}

// Function to write the Lexicon in text format
void writeLexiconText(const string& lexiconFilePath, const vector<LexiconEntry>& lexicon) {
    ofstream lexFile(lexiconFilePath);
    if (!lexFile.is_open()) {
        throw runtime_error("Failed to open lexicon file for writing: " + lexiconFilePath);
    }

    for (const auto& entry : lexicon) {
        // Write term, offset, length, and docFreq separated by spaces
        lexFile << entry.term << " " << entry.offset << " " << entry.length << " " << entry.docFreq << "\n";
    }

    lexFile.close();
}

// Function to turn int to bytes
vector<uint8_t> intToVarByte(int num) {
    vector<uint8_t> bytes;

    while (num > 0) {
        // Extract the 7 least significant bits
        uint8_t byte = num & 0x7F;
        // Set the high bit to indicate more bytes if needed
        if (num > 0x7F)
        {
            byte |= 0x80;
        }
        bytes.push_back(byte);
        // Shift the value to the right by 7 bits
        num >>= 7;
    }
    return bytes;
}

//function to turn bytes to int
int byteToInt(const vector<uint8_t>& bytes) {
    int value = 0;
    int shift = 0;

    for (size_t i = 0; i < bytes.size(); ++i) {
        value |= (bytes[i] & 0x7F) << shift; // Mask the highest bit
        shift += 7;

        if (bytes[i] & 0x80) {  // If the highest bit is 1, continue
            continue;
        }
        break; // Exit if the highest bit is 0
    }

    return value;
}
   

// Function to perform k-way merge and build the final inverted index with Differential Encoding and Non-Interleaved Storage
void mergePostingFiles(const vector<string>& files, const string& indexFilePath,
                      const string& lexiconFilePath,
                      vector<LexiconEntry>& lexicon,
                      vector<BlockMetaData>& blockMetaData) {
    // Initialize readers
    vector<unique_ptr<PostingFileReader>> readers;
    for (const auto& file : files) {
        readers.emplace_back(make_unique<PostingFileReader>(file));
    }

    // Initialize priority queue
    priority_queue<pair<string, size_t>,
                        vector<pair<string, size_t>>,
                        ComparePQNode> minHeap;

    
    // Insert the first term from each reader into the heap
    for (size_t i = 0; i < readers.size(); ++i) {
        if (readers[i]->hasNext()) {
            minHeap.emplace(readers[i]->getCurrentTerm(), i);
        }
    }

    // Open final index file for writing in text format
    ofstream indexFile(indexFilePath, ios::binary);
    if (!indexFile.is_open()) {
        throw runtime_error("Failed to open final index file for writing: " + indexFilePath);
    }

    uint64_t currentOffset = 0; // Byte offset in the index file

    vector<uint8_t> combinedIndexBytes;
    vector<int> combinedDocID;
    vector<int> combinedFreq;

    while (!minHeap.empty()) {
        auto [smallestTerm, fileIdx] = minHeap.top();
        minHeap.pop();

        // Collect all postings for the smallest term from all readers
        vector<Posting> mergedPostings = readers[fileIdx]->getCurrentPostings();

        // Advance the reader and add the next term to the heap
        readers[fileIdx]->readNextTerm();
        if (readers[fileIdx]->hasNext()) {
            minHeap.emplace(readers[fileIdx]->getCurrentTerm(), fileIdx);
        }

        // Check for other readers with the same term
        while (!minHeap.empty() && minHeap.top().first == smallestTerm) {
            auto [sameTerm, sameFileIdx] = minHeap.top();
            minHeap.pop();
            // Merge postings from the same term
            const auto& samePostings = readers[sameFileIdx]->getCurrentPostings();
            mergedPostings.insert(mergedPostings.end(), samePostings.begin(), samePostings.end());

            // Advance the reader and add the next term to the heap
            readers[sameFileIdx]->readNextTerm();
            if (readers[sameFileIdx]->hasNext()) {
                minHeap.emplace(readers[sameFileIdx]->getCurrentTerm(), sameFileIdx);
            }
        }

        // Differential Encoding for docIDs
        int previousDocID = 0;
        int currDocID;
        int lastBlockID;
        int postingCount = 0;

        LexiconEntry lexEntry;
        lexEntry.term = smallestTerm;
        lexEntry.offset = currentOffset;
        lexEntry.length = 0;
        
        for (auto& posting : mergedPostings) {
            
            currDocID = posting.docID;
            //if the posting is more than POSTING_PER_BLOCK, we block them
            if (postingCount % POSTING_PER_BLOCK == 0 && postingCount != 0) {
                BlockMetaData currBlock;
                currBlock.size = 0;
                currBlock.lastDocID = lastBlockID;
                for (const auto& docID : combinedDocID) {
                    vector<uint8_t> varbytes = intToVarByte(docID);
                    combinedIndexBytes.insert(combinedIndexBytes.end(), varbytes.begin(), varbytes.end());
                }
                for (const auto& freq : combinedFreq) {
                    vector<uint8_t> varbytes = intToVarByte(freq);
                    combinedIndexBytes.insert(combinedIndexBytes.end(), varbytes.begin(), varbytes.end());
                }
                indexFile.write(reinterpret_cast<char*>(combinedIndexBytes.data()), combinedIndexBytes.size());
                
                currBlock.size = static_cast<uint32_t>(combinedIndexBytes.size());
                lexEntry.length += static_cast<uint32_t>(combinedIndexBytes.size());
                if (postingCount == mergedPostings.size()-1) {
                    lastBlockID = currDocID;
                }
                
                blockMetaData.push_back(currBlock);
                combinedIndexBytes.clear();
                combinedDocID.clear();
                combinedFreq.clear();
            }
            else if (postingCount % POSTING_PER_BLOCK == POSTING_PER_BLOCK - 1) {
                lastBlockID = currDocID;
            }

            //take the difference of postings 
            posting.docID -= previousDocID;
            
            previousDocID = currDocID;
    
            combinedDocID.push_back(posting.docID);
            combinedFreq.push_back(posting.termFreq);
            postingCount++;
            
        }

        //write out remaining index
        if (!combinedDocID.empty()) {
            BlockMetaData currBlock;
            currBlock.size = 0;
            currBlock.lastDocID = combinedDocID[-1];
            for (const auto& docID : combinedDocID) {
                vector<uint8_t> varbytes = intToVarByte(docID);
                combinedIndexBytes.insert(combinedIndexBytes.end(), varbytes.begin(), varbytes.end());
            }
            for (const auto& freq : combinedFreq) {
                vector<uint8_t> varbytes = intToVarByte(freq);
                combinedIndexBytes.insert(combinedIndexBytes.end(), varbytes.begin(), varbytes.end());
            }
            indexFile.write(reinterpret_cast<char*>(combinedIndexBytes.data()), combinedIndexBytes.size());
            
            currBlock.size = static_cast<uint32_t>(combinedIndexBytes.size());
            lexEntry.length += static_cast<uint32_t>(combinedIndexBytes.size());
            blockMetaData.push_back(currBlock);
            combinedIndexBytes.clear();
            combinedDocID.clear();
            combinedFreq.clear();
        }

        lexEntry.docFreq = mergedPostings.size();

        // Add to lexicon
        lexicon.emplace_back(lexEntry);

        // Update the currentOffset
        currentOffset += lexEntry.length;
    }

    

    indexFile.close();
}

void writeBlockMetaData(const string& blockMetaDataFilePath, const vector<BlockMetaData>& blockMetaData) {
    // Open block meta data file for writing in text format
    ofstream metaFile(blockMetaDataFilePath);
    if (!metaFile.is_open()) {
        throw runtime_error("Failed to open block meta data file for writing: " + blockMetaDataFilePath);
    }

    for (const auto& metaData : blockMetaData) {
        //write block meata data
        metaFile << metaData.size << " " << metaData.lastDocID << "\n";
    }

    metaFile.close();
    
}

int main() {

    // Start the timer
    auto start = chrono::high_resolution_clock::now();

    string intermediateDir = "src/temp";
    string finalIndexDir = "src/index_4";

    // Check if intermediate directory exists
    if (!fs::exists(intermediateDir) || !fs::is_directory(intermediateDir)) {
        cerr << "Intermediate directory does not exist or is not a directory: " << intermediateDir << endl;
        return EXIT_FAILURE;
    }

    // Create final index directory if it doesn't exist
    try {
        if (!fs::exists(finalIndexDir)) {
            fs::create_directories(finalIndexDir);
            cout << "Created final index directory: " << finalIndexDir << endl;
        } else {
            if (!fs::is_directory(finalIndexDir)) {
                cerr << "Final index path exists and is not a directory: " << finalIndexDir << endl;
                return EXIT_FAILURE;
            }
        }
    } catch (const fs::filesystem_error& e) {
        cerr << "Filesystem error: " << e.what() << endl;
        return EXIT_FAILURE;
    }

    // List intermediate text files
    vector<string> intermediateFiles = listIntermediateFiles(intermediateDir);
    if (intermediateFiles.empty()) {
        cerr << "No intermediate text files found in directory: " << intermediateDir << endl;
        return EXIT_FAILURE;
    }

    cout << "Found " << intermediateFiles.size() << " intermediate files." << endl;

    // Merge posting files
    string finalIndexPath = finalIndexDir + "/index.bin";
    string lexiconPath = finalIndexDir + "/lexicon.txt";
    string BlockMetaDataFilePath = finalIndexDir + "/blockMetaData.txt";
    vector<LexiconEntry> lexicon;
    vector<BlockMetaData> blockMetaData;
    try {
        mergePostingFiles(intermediateFiles, finalIndexPath, lexiconPath, lexicon, blockMetaData);
        cout << "Merged postings into final index file: " << finalIndexPath << endl;
    } catch (const exception& ex) {
        cerr << "Error during merging: " << ex.what() << endl;
        return EXIT_FAILURE;
    }
    
    // Write lexicon in text format
    try {
        writeLexiconText(lexiconPath, lexicon);
        cout << "Written lexicon file: " << lexiconPath << endl;
    } catch (const exception& ex) {
        cerr << "Error writing lexicon: " << ex.what() << endl;
        return EXIT_FAILURE;
    }

    cout << "Merger completed successfully." << endl;
    cout << "index.txt output format: " << endl;
    cout << "DocID1 gapDocID2 ... termFreq1 termFreq2 ..." << endl;
    cout << "loxicon.txt output format: " << endl;
    cout << "term offset length docFreq" << endl;

    // Write block meta data in text format
    try {
        writeBlockMetaData(BlockMetaDataFilePath, blockMetaData);
        cout << "Written block meta data file: " << BlockMetaDataFilePath << endl;
    } catch (const exception& ex) {
        cerr << "Error writing block meta data: " << ex.what() << endl;
        return EXIT_FAILURE;
    }

    cout << "blockMetaData.txt output format: " << endl;
    cout << "block1size block1lastDocID ... " << endl;

    // Stop the timer
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> duration = end - start;
    cout << "Execution time: " << duration.count() << " seconds." << std::endl;
    

    return EXIT_SUCCESS;
}
