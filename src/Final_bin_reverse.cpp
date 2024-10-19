// bin_reverse.cpp
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>
#include <stdexcept>
#include <filesystem>

// Namespace alias for filesystem
namespace fs = std::filesystem;

// Struct definitions
struct LexiconEntry {
    std::string term;
    uint64_t offset;     // Offset in the final inverted index file
    uint32_t length;     // Length of the posting list in bytes
    uint32_t docFreq;    // Number of documents containing the term
};

struct Posting {
    int docID;
    int termFreq;
};

// Function to perform VarByte decoding on a stream of bytes
int varbyteDecode(std::ifstream& infile) {
    int number = 0;
    int shift = 0;
    uint8_t byte;
    while (infile.read(reinterpret_cast<char*>(&byte), 1)) {
        number |= (byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }
    return number;
}

// Function to read the lexicon
std::vector<LexiconEntry> readLexicon(const std::string& lexiconFilename) {
    std::vector<LexiconEntry> lexicon;
    std::ifstream lexFile(lexiconFilename, std::ios::binary);
    if (!lexFile.is_open()) {
        throw std::runtime_error("Failed to open lexicon file: " + lexiconFilename);
    }

    while (lexFile.peek() != EOF) {
        // Read term length
        uint32_t termLength;
        lexFile.read(reinterpret_cast<char*>(&termLength), sizeof(termLength));
        if (lexFile.eof()) break;

        // Read term
        std::string term(termLength, '\0');
        lexFile.read(&term[0], termLength);

        // Read offset, length, and docFreq
        uint64_t offset;
        uint32_t length, docFreq;
        lexFile.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        lexFile.read(reinterpret_cast<char*>(&length), sizeof(length));
        lexFile.read(reinterpret_cast<char*>(&docFreq), sizeof(docFreq));

        lexicon.push_back(LexiconEntry{term, offset, length, docFreq});
    }

    lexFile.close();
    return lexicon;
}

// Function to read postings from index.bin based on offset and length
std::vector<Posting> readPostings(std::ifstream& indexFile, uint64_t offset, uint32_t length) {
    std::vector<Posting> postings;
    indexFile.seekg(offset, std::ios::beg);
    if (!indexFile.good()) {
        throw std::runtime_error("Failed to seek to offset: " + std::to_string(offset));
    }

    // Read number of postings
    uint32_t numPostings;
    indexFile.read(reinterpret_cast<char*>(&numPostings), sizeof(numPostings));

    // Read postings
    for (uint32_t i = 0; i < numPostings; ++i) {
        int docID = varbyteDecode(indexFile);
        int termFreq = varbyteDecode(indexFile);
        postings.emplace_back(Posting{docID, termFreq});
    }

    return postings;
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: ./reverse_indexer <index.bin> <lexicon.bin> <output.txt>" << std::endl;
        return EXIT_FAILURE;
    }

    std::string indexFilename = argv[1];
    std::string lexiconFilename = argv[2];
    std::string outputFilename = argv[3];

    // Check if input files exist
    if (!fs::exists(indexFilename)) {
        std::cerr << "Index file does not exist: " << indexFilename << std::endl;
        return EXIT_FAILURE;
    }
    if (!fs::exists(lexiconFilename)) {
        std::cerr << "Lexicon file does not exist: " << lexiconFilename << std::endl;
        return EXIT_FAILURE;
    }

    try {
        // Read lexicon entries
        std::vector<LexiconEntry> lexicon = readLexicon(lexiconFilename);
        std::cout << "Total terms in lexicon: " << lexicon.size() << std::endl;

        // Open index.bin for reading
        std::ifstream indexFile(indexFilename, std::ios::binary);
        if (!indexFile.is_open()) {
            throw std::runtime_error("Failed to open index file: " + indexFilename);
        }

        // Open output.txt for writing
        std::ofstream outfile(outputFilename);
        if (!outfile.is_open()) {
            throw std::runtime_error("Failed to open ASCII output file for writing: " + outputFilename);
        }

        // Iterate through lexicon and write to ASCII
        for (const auto& entry : lexicon) {
            // Read postings for the current term
            std::vector<Posting> postings = readPostings(indexFile, entry.offset, entry.length);

            // Write term to ASCII file
            outfile << entry.term;

            // Write postings in "docID:termFreq" format
            for (const auto& posting : postings) {
                outfile << " " << posting.docID << ":" << posting.termFreq;
            }

            // End the line for this term
            outfile << "\n";
        }

        indexFile.close();
        outfile.close();

        std::cout << "Binary files successfully converted to ASCII: " << outputFilename << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "Error during conversion: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
