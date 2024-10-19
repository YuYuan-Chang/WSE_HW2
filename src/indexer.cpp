// indexer.cpp
#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>
#include <cctype>
#include <cstdlib>
#include <unordered_set>
#include <filesystem> // Include filesystem library

namespace fs = std::filesystem; // Alias for convenience

// Posting structure: docID and term frequency
struct Posting {
    int docID;
    int termFreq;
};

// Function to tokenize text with stop words removal and ASCII check
std::vector<std::string> tokenize(const std::string& text, const std::unordered_set<std::string>& stopWords) {
    std::vector<std::string> tokens;
    std::string token;
    auto isASCII = [](const std::string& token) -> bool {
        for (char c : token) {
            if (static_cast<unsigned char>(c) > 127) return false;
        }
        return true;
    };

    for (char c : text) {
        if (std::isalnum(static_cast<unsigned char>(c))) {
            token += std::tolower(static_cast<unsigned char>(c));
        } else if (!token.empty()) {
            if (isASCII(token) && stopWords.find(token) == stopWords.end()) {
                tokens.push_back(token);
            }
            token.clear();
        }
    }
    if (!token.empty() && isASCII(token) && stopWords.find(token) == stopWords.end()) {
        tokens.push_back(token);
    }
    return tokens;
}

// Function to perform VarByte encoding on a single integer
std::vector<uint8_t> varbyteEncode(int number) {
    std::vector<uint8_t> bytes;
    while (number >= 128) {
        bytes.push_back((number & 0x7F) | 0x80);
        number >>= 7;
    }
    bytes.push_back(number & 0x7F);
    return bytes;
}

// Function to write intermediate posting file in binary format
void writeBinaryPostingFile(const std::string& filename, 
                            const std::unordered_map<std::string, std::vector<Posting>>& invertedIndex) {
    std::ofstream outfile(filename, std::ios::binary);
    if (!outfile.is_open()) {
        throw std::runtime_error("Failed to open binary intermediate file for writing: " + filename);
    }

    for (const auto& [term, postings] : invertedIndex) {
        // Write term length and term
        uint32_t termLength = term.size();
        outfile.write(reinterpret_cast<const char*>(&termLength), sizeof(termLength));
        outfile.write(term.c_str(), term.size());

        // Write number of postings
        uint32_t numPostings = postings.size();
        outfile.write(reinterpret_cast<const char*>(&numPostings), sizeof(numPostings));

        // Write postings with VarByte encoding
        for (const auto& posting : postings) {
            std::vector<uint8_t> encodedDocID = varbyteEncode(posting.docID);
            std::vector<uint8_t> encodedFreq = varbyteEncode(posting.termFreq);
            outfile.write(reinterpret_cast<const char*>(encodedDocID.data()), encodedDocID.size());
            outfile.write(reinterpret_cast<const char*>(encodedFreq.data()), encodedFreq.size());
        }
    }

    outfile.close();
}

// Function to parse the collection and create intermediate posting files
void parseCollection(const std::string& filepath, 
                    std::unordered_map<std::string, std::vector<Posting>>& invertedIndex,
                    int& currentBlockSize,
                    const int maxBlockSize,
                    int& blockCount,
                    const std::string& outputDir,
                    const std::unordered_set<std::string>& stopWords) {
    std::ifstream infile(filepath);
    if (!infile.is_open()) {
        throw std::runtime_error("Failed to open collection file: " + filepath);
    }

    std::string line;
    static int processedDocs = 0; // Document counter
    while (std::getline(infile, line)) {
        // Split the line into docID and passage
        size_t tabPos = line.find('\t');
        if (tabPos == std::string::npos) {
            continue; // Skip malformed lines
        }

        int docID = std::stoi(line.substr(0, tabPos));
        std::string passage = line.substr(tabPos + 1);

        // Tokenize the passage
        std::vector<std::string> tokens = tokenize(passage, stopWords);

        // Count term frequencies in the current document
        std::unordered_map<std::string, int> termFreqMap;
        for (const auto& token : tokens) {
            termFreqMap[token]++;
        }

        // Update the inverted index
        for (const auto& [term, freq] : termFreqMap) {
            invertedIndex[term].emplace_back(Posting{docID, freq});
            // Estimate block size: term length + (docID + freq) bytes
            currentBlockSize += term.size() + sizeof(int) * 2;
        }

        // Increment document counter and log progress
        processedDocs++;
        if (processedDocs % 100000 == 0) {
            std::cout << "Processed " << processedDocs << " documents..." << std::endl;
        }

        // Check if the current block size exceeds the maximum allowed
        if (currentBlockSize >= maxBlockSize) {
            // Write the current inverted index to an intermediate file
            std::string filename = outputDir + "/intermediate_" + std::to_string(blockCount++) + ".bin";
            writeBinaryPostingFile(filename, invertedIndex);
            std::cout << "Written intermediate file: " << filename << std::endl;

            // Clear the in-memory inverted index and reset block size
            invertedIndex.clear();
            currentBlockSize = 0;
        }
    }

    infile.close();

    // Write any remaining postings to an intermediate file
    if (!invertedIndex.empty()) {
        std::string filename = outputDir + "/intermediate_" + std::to_string(blockCount++) + ".bin";
        writeBinaryPostingFile(filename, invertedIndex);
        std::cout << "Written intermediate file: " << filename << std::endl;
    }
}

int main(int argc, char* argv[]) {
    // The indexer now expects exactly two arguments: <collection.tsv> and <output_dir>
    if (argc != 3) {
        std::cerr << "Usage: ./indexer <collection.tsv> <output_dir>" << std::endl;
        return EXIT_FAILURE;
    }

    std::string collectionPath = argv[1];
    std::string outputDir = argv[2];

    // Check if output directory exists; if not, create it
    try {
        if (!fs::exists(outputDir)) {
            fs::create_directories(outputDir);
            std::cout << "Created output directory: " << outputDir << std::endl;
        } else {
            // Optional: Check if it's a directory
            if (!fs::is_directory(outputDir)) {
                std::cerr << "Output path exists and is not a directory: " << outputDir << std::endl;
                return EXIT_FAILURE;
            }
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    // Initialize variables
    std::unordered_map<std::string, std::vector<Posting>> invertedIndex;
    int currentBlockSize = 0;
    const int maxBlockSize = 100 * 1024 * 1024; // 100 MB per block
    int blockCount = 0;

    // Initialize stop words
    std::unordered_set<std::string> stopWords = {
        "the", "is", "at", "which", "on", "and", "a", "an", "of", "or", "in", "to", "with",
        "was", "as", "by", "for", "from", "that", "this", "it", "its", "be", "are", "but",
        "not", "have", "has", "had", "were", "been", "their", "they", "them"
        // Add more stop words as needed
    };

    try {
        parseCollection(collectionPath, invertedIndex, currentBlockSize, maxBlockSize, blockCount, outputDir, stopWords);
        std::cout << "Indexing completed successfully. " << blockCount << " intermediate files created." << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "Error during indexing: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
