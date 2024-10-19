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

// Namespace alias for convenience
namespace fs = std::filesystem;

// Struct definitions
struct Posting {
    int docID;
    int termFreq;
};

struct LexiconEntry {
    std::string term;
    uint64_t offset;     // Offset in the final inverted index file
    uint32_t length;     // Length of the posting list in bytes
    uint32_t docFreq;    // Number of documents containing the term
};

struct PageTableEntry {
    int docID;
    std::string metadata; // e.g., URL
};

// PostingFileReader class to handle reading from intermediate binary files
class PostingFileReader {
public:
    PostingFileReader(const std::string& filepath) : infile(filepath, std::ios::binary), eof(false) {
        if (!infile.is_open()) {
            throw std::runtime_error("Failed to open intermediate file: " + filepath);
        }
        readNextTerm();
    }

    bool hasNext() const {
        return !eof;
    }

    const std::string& getCurrentTerm() const {
        return currentTerm;
    }

    const std::vector<Posting>& getCurrentPostings() const {
        return currentPostings;
    }

    void readNextTerm() {
        if (!infile.good()) {
            eof = true;
            return;
        }

        // Read term length
        uint32_t termLength;
        infile.read(reinterpret_cast<char*>(&termLength), sizeof(termLength));
        if (infile.eof()) {
            eof = true;
            return;
        }

        // Read term
        currentTerm.resize(termLength);
        infile.read(&currentTerm[0], termLength);

        // Read number of postings
        uint32_t numPostings;
        infile.read(reinterpret_cast<char*>(&numPostings), sizeof(numPostings));

        // Read postings
        currentPostings.clear();
        for (uint32_t i = 0; i < numPostings; ++i) {
            // Read VarByte encoded docID
            int docID = 0;
            while (true) {
                uint8_t byte;
                infile.read(reinterpret_cast<char*>(&byte), sizeof(byte));
                if (infile.eof()) {
                    throw std::runtime_error("Unexpected EOF while reading docID.");
                }
                docID = (docID << 7) | (byte & 0x7F);
                if ((byte & 0x80) == 0) break;
            }

            // Read VarByte encoded termFreq
            int termFreq = 0;
            while (true) {
                uint8_t byte;
                infile.read(reinterpret_cast<char*>(&byte), sizeof(byte));
                if (infile.eof()) {
                    throw std::runtime_error("Unexpected EOF while reading termFreq.");
                }
                termFreq = (termFreq << 7) | (byte & 0x7F);
                if ((byte & 0x80) == 0) break;
            }

            currentPostings.emplace_back(Posting{docID, termFreq});
        }
    }

private:
    std::ifstream infile;
    bool eof;
    std::string currentTerm;
    std::vector<Posting> currentPostings;
};

// Comparator for the priority queue (min heap based on term)
struct ComparePQNode {
    bool operator()(const std::pair<std::string, size_t>& a, const std::pair<std::string, size_t>& b) {
        return a.first > b.first; // Min heap
    }
};

// Function to list intermediate binary files
std::vector<std::string> listIntermediateFiles(const std::string& directory) {
    std::vector<std::string> files;
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (entry.is_regular_file() && entry.path().extension() == ".bin") {
            files.push_back(entry.path().string());
        }
    }
    // Sort files to ensure consistent merging order
    std::sort(files.begin(), files.end());
    return files;
}

// Function to write the Lexicon
void writeLexicon(const std::string& lexiconFilePath, const std::vector<LexiconEntry>& lexicon) {
    std::ofstream lexFile(lexiconFilePath, std::ios::binary);
    if (!lexFile.is_open()) {
        throw std::runtime_error("Failed to open lexicon file for writing: " + lexiconFilePath);
    }

    for (const auto& entry : lexicon) {
        // Write term length and term
        uint32_t termLength = entry.term.size();
        lexFile.write(reinterpret_cast<const char*>(&termLength), sizeof(termLength));
        lexFile.write(entry.term.c_str(), entry.term.size());

        // Write offset, length, and docFreq
        lexFile.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
        lexFile.write(reinterpret_cast<const char*>(&entry.length), sizeof(entry.length));
        lexFile.write(reinterpret_cast<const char*>(&entry.docFreq), sizeof(entry.docFreq));
    }

    lexFile.close();
}

// Function to build the Page Table from the collection file
std::vector<PageTableEntry> buildPageTable(const std::string& collectionPath) {
    std::vector<PageTableEntry> pageTable;
    std::ifstream infile(collectionPath);
    if (!infile.is_open()) {
        throw std::runtime_error("Failed to open collection file for building Page Table: " + collectionPath);
    }

    std::string line;
    while (std::getline(infile, line)) {
        size_t tabPos = line.find('\t');
        if (tabPos == std::string::npos) {
            continue; // Skip malformed lines
        }

        int docID = std::stoi(line.substr(0, tabPos));
        std::string passage = line.substr(tabPos + 1);

        // Extract metadata as needed; assuming URL is part of the passage or elsewhere
        // For simplicity, using passage as metadata here
        pageTable.emplace_back(PageTableEntry{docID, passage});
    }

    infile.close();
    return pageTable;
}

// Function to write the Page Table
void writePageTable(const std::string& pageTablePath, const std::vector<PageTableEntry>& pageTable) {
    std::ofstream pageFile(pageTablePath, std::ios::binary);
    if (!pageFile.is_open()) {
        throw std::runtime_error("Failed to open Page Table file for writing: " + pageTablePath);
    }

    for (const auto& entry : pageTable) {
        // Write docID
        pageFile.write(reinterpret_cast<const char*>(&entry.docID), sizeof(entry.docID));

        // Write metadata length and metadata
        uint32_t metaLength = entry.metadata.size();
        pageFile.write(reinterpret_cast<const char*>(&metaLength), sizeof(metaLength));
        pageFile.write(entry.metadata.c_str(), entry.metadata.size());
    }

    pageFile.close();
}

// Function to perform k-way merge and build the final inverted index
void mergePostingFiles(const std::vector<std::string>& files, const std::string& indexFilePath,
                      std::vector<LexiconEntry>& lexicon) {
    // Initialize readers
    std::vector<std::unique_ptr<PostingFileReader>> readers;
    for (const auto& file : files) {
        readers.emplace_back(std::make_unique<PostingFileReader>(file));
    }

    // Initialize priority queue
    std::priority_queue<std::pair<std::string, size_t>,
                        std::vector<std::pair<std::string, size_t>>,
                        ComparePQNode> minHeap;

    // Insert the first term from each reader into the heap
    for (size_t i = 0; i < readers.size(); ++i) {
        if (readers[i]->hasNext()) {
            minHeap.emplace(readers[i]->getCurrentTerm(), i);
        }
    }

    // Open final index file for writing
    std::ofstream indexFile(indexFilePath, std::ios::binary);
    if (!indexFile.is_open()) {
        throw std::runtime_error("Failed to open final index file for writing: " + indexFilePath);
    }

    uint64_t currentOffset = 0;

    while (!minHeap.empty()) {
        auto [smallestTerm, fileIdx] = minHeap.top();
        minHeap.pop();

        // Collect all postings for the smallest term from all readers
        std::vector<Posting> mergedPostings = readers[fileIdx]->getCurrentPostings();

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

        // Sort the merged postings by docID
        std::sort(mergedPostings.begin(), mergedPostings.end(),
                  [](const Posting& a, const Posting& b) -> bool {
                      return a.docID < b.docID;
                  });

        // Remove duplicate docIDs by summing term frequencies
        std::vector<Posting> uniquePostings;
        if (!mergedPostings.empty()) {
            uniquePostings.emplace_back(mergedPostings[0]);
            for (size_t i = 1; i < mergedPostings.size(); ++i) {
                if (mergedPostings[i].docID == uniquePostings.back().docID) {
                    uniquePostings.back().termFreq += mergedPostings[i].termFreq;
                } else {
                    uniquePostings.emplace_back(mergedPostings[i]);
                }
            }
        }

        // Record lexicon entry
        LexiconEntry lexEntry;
        lexEntry.term = smallestTerm;
        lexEntry.offset = currentOffset;
        lexEntry.docFreq = uniquePostings.size();

        // Write postings to the index file with VarByte encoding
        // Write number of postings
        uint32_t numPostings = uniquePostings.size();
        indexFile.write(reinterpret_cast<const char*>(&numPostings), sizeof(numPostings));

        // Write postings
        for (const auto& posting : uniquePostings) {
            // VarByte encode docID
            std::vector<uint8_t> encodedDocID;
            int number = posting.docID;
            while (number >= 128) {
                encodedDocID.push_back((number & 0x7F) | 0x80);
                number >>= 7;
            }
            encodedDocID.push_back(number & 0x7F);
            indexFile.write(reinterpret_cast<const char*>(encodedDocID.data()), encodedDocID.size());

            // VarByte encode termFreq
            std::vector<uint8_t> encodedFreq;
            number = posting.termFreq;
            while (number >= 128) {
                encodedFreq.push_back((number & 0x7F) | 0x80);
                number >>= 7;
            }
            encodedFreq.push_back(number & 0x7F);
            indexFile.write(reinterpret_cast<const char*>(encodedFreq.data()), encodedFreq.size());

            currentOffset += encodedDocID.size() + encodedFreq.size();
        }

        // Set length in bytes for lexicon
        lexEntry.length = currentOffset - lexEntry.offset;

        // Add to lexicon
        lexicon.emplace_back(lexEntry);
    }

    indexFile.close();
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: ./merger <intermediate_dir> <final_index_dir> <collection.tsv>" << std::endl;
        return EXIT_FAILURE;
    }

    std::string intermediateDir = argv[1];
    std::string finalIndexDir = argv[2];
    std::string collectionPath = argv[3];

    // Check if intermediate directory exists
    if (!fs::exists(intermediateDir) || !fs::is_directory(intermediateDir)) {
        std::cerr << "Intermediate directory does not exist or is not a directory: " << intermediateDir << std::endl;
        return EXIT_FAILURE;
    }

    // Create final index directory if it doesn't exist
    try {
        if (!fs::exists(finalIndexDir)) {
            fs::create_directories(finalIndexDir);
            std::cout << "Created final index directory: " << finalIndexDir << std::endl;
        } else {
            if (!fs::is_directory(finalIndexDir)) {
                std::cerr << "Final index path exists and is not a directory: " << finalIndexDir << std::endl;
                return EXIT_FAILURE;
            }
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    // List intermediate binary files
    std::vector<std::string> intermediateFiles = listIntermediateFiles(intermediateDir);
    if (intermediateFiles.empty()) {
        std::cerr << "No intermediate binary files found in directory: " << intermediateDir << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Found " << intermediateFiles.size() << " intermediate files." << std::endl;

    // Merge posting files
    std::string finalIndexPath = finalIndexDir + "/index.bin";
    std::vector<LexiconEntry> lexicon;
    try {
        mergePostingFiles(intermediateFiles, finalIndexPath, lexicon);
        std::cout << "Merged postings into final index file: " << finalIndexPath << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "Error during merging: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    // Write lexicon
    std::string lexiconPath = finalIndexDir + "/lexicon.bin";
    try {
        writeLexicon(lexiconPath, lexicon);
        std::cout << "Written lexicon file: " << lexiconPath << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "Error writing lexicon: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    // Build and write Page Table
    std::cout << "Building Page Table..." << std::endl;
    std::vector<PageTableEntry> pageTable;
    try {
        pageTable = buildPageTable(collectionPath);
        std::cout << "Built Page Table with " << pageTable.size() << " entries." << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "Error building Page Table: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    std::string pageTablePath = finalIndexDir + "/pagetable.bin";
    try {
        writePageTable(pageTablePath, pageTable);
        std::cout << "Written Page Table file: " << pageTablePath << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "Error writing Page Table: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Merger completed successfully." << std::endl;
    return EXIT_SUCCESS;
}
