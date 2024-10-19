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
    uint32_t length;     // Length of the postings list in bytes
    uint32_t docFreq;    // Number of documents containing the term
};

struct PageTableEntry {
    int docID;
    std::string metadata; // e.g., URL
};

// PostingFileReader class to handle reading from intermediate text files
class PostingFileReader {
public:
    PostingFileReader(const std::string& filepath) : infile(filepath), eof(false) {
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
        if (!std::getline(infile, currentLine)) {
            eof = true;
            return;
        }

        // Parse the current line
        std::istringstream iss(currentLine);
        iss >> currentTerm;

        currentPostings.clear();
        std::string postingStr;
        while (iss >> postingStr) {
            size_t colonPos = postingStr.find(':');
            if (colonPos == std::string::npos) {
                throw std::runtime_error("Malformed posting: " + postingStr);
            }

            int docID = std::stoi(postingStr.substr(0, colonPos));
            int termFreq = std::stoi(postingStr.substr(colonPos + 1));

            // Validate docID and termFreq
            if (docID < 0 || termFreq < 0) {
                throw std::runtime_error("Invalid docID or termFreq in posting: " + postingStr);
            }

            currentPostings.emplace_back(Posting{docID, termFreq});
        }
    }

private:
    std::ifstream infile;
    bool eof;
    std::string currentLine;
    std::string currentTerm;
    std::vector<Posting> currentPostings;
};

// Comparator for the priority queue (min heap based on term)
struct ComparePQNode {
    bool operator()(const std::pair<std::string, size_t>& a, const std::pair<std::string, size_t>& b) {
        return a.first > b.first; // Min heap
    }
};

// Function to list intermediate text files
std::vector<std::string> listIntermediateFiles(const std::string& directory) {
    std::vector<std::string> files;
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (entry.is_regular_file() && entry.path().extension() == ".txt") {
            files.push_back(entry.path().string());
        }
    }
    // Sort files to ensure consistent merging order
    std::sort(files.begin(), files.end());
    return files;
}

// Function to write the Lexicon in text format
void writeLexiconText(const std::string& lexiconFilePath, const std::vector<LexiconEntry>& lexicon) {
    std::ofstream lexFile(lexiconFilePath);
    if (!lexFile.is_open()) {
        throw std::runtime_error("Failed to open lexicon file for writing: " + lexiconFilePath);
    }

    for (const auto& entry : lexicon) {
        // Write term, offset, length, and docFreq separated by spaces
        lexFile << entry.term << " " << entry.offset << " " << entry.length << " " << entry.docFreq << "\n";
    }

    lexFile.close();
}

// Function to build the Page Table from the collection file (unchanged)
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

// Function to write the Page Table (unchanged; assuming binary format)
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
                      const std::string& lexiconFilePath,
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

    // Open final index file for writing in text format
    std::ofstream indexFile(indexFilePath);
    if (!indexFile.is_open()) {
        throw std::runtime_error("Failed to open final index file for writing: " + indexFilePath);
    }

    uint64_t currentOffset = 0; // Byte offset in the index file

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

        // Write the term and its postings to the final index file
        indexFile << lexEntry.term;

        // Write postings
        for (const auto& posting : uniquePostings) {
            indexFile << " " << posting.docID << ":" << posting.termFreq;
        }

        // Write newline
        indexFile << "\n";

        // Calculate the number of bytes written for this line
        // Each character is 1 byte in ASCII
        // Term + space + postings + newline
        std::string line = lexEntry.term;
        for (const auto& posting : uniquePostings) {
            line += " " + std::to_string(posting.docID) + ":" + std::to_string(posting.termFreq);
        }
        line += "\n";

        lexEntry.length = line.size();
        currentOffset += lexEntry.length;

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

    // List intermediate text files
    std::vector<std::string> intermediateFiles = listIntermediateFiles(intermediateDir);
    if (intermediateFiles.empty()) {
        std::cerr << "No intermediate text files found in directory: " << intermediateDir << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Found " << intermediateFiles.size() << " intermediate files." << std::endl;

    // Merge posting files
    std::string finalIndexPath = finalIndexDir + "/index.txt";
    std::string lexiconPath = finalIndexDir + "/lexicon.txt";
    std::vector<LexiconEntry> lexicon;
    try {
        mergePostingFiles(intermediateFiles, finalIndexPath, lexiconPath, lexicon);
        std::cout << "Merged postings into final index file: " << finalIndexPath << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "Error during merging: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    // Write lexicon in text format
    try {
        writeLexiconText(lexiconPath, lexicon);
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
    std::cout << "<term> <offset> <length> <docFreq>" << std::endl;
    return EXIT_SUCCESS;
}
