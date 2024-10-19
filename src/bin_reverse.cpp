#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <stdexcept>

// Posting structure: docID and term frequency
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

// Function to reverse the binary file and convert it to ASCII
void reverseBinaryToASCII(const std::string& binaryFilename, const std::string& outputFilename) {
    std::ifstream infile(binaryFilename, std::ios::binary);
    if (!infile.is_open()) {
        throw std::runtime_error("Failed to open binary file for reading.");
    }

    std::ofstream outfile(outputFilename);
    if (!outfile.is_open()) {
        throw std::runtime_error("Failed to open ASCII output file for writing.");
    }

    while (infile.peek() != EOF) {
        // Read term length
        uint32_t termLength;
        infile.read(reinterpret_cast<char*>(&termLength), sizeof(termLength));

        // Read the term
        std::string term(termLength, '\0');
        infile.read(term.data(), termLength);

        // Read the number of postings
        uint32_t numPostings;
        infile.read(reinterpret_cast<char*>(&numPostings), sizeof(numPostings));

        // Write the term to the ASCII file
        outfile << term;

        // Read and decode postings (docID and term frequency)
        for (uint32_t i = 0; i < numPostings; ++i) {
            int docID = varbyteDecode(infile);
            int termFreq = varbyteDecode(infile);

            // Write docID and term frequency to the ASCII file
            outfile << " " << docID << ":" << termFreq;
        }

        // End the line for this term
        outfile << "\n";
    }

    infile.close();
    outfile.close();
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: ./reverse_indexer <input.bin> <output.txt>" << std::endl;
        return EXIT_FAILURE;
    }

    std::string binaryFilename = argv[1];
    std::string outputFilename = argv[2];

    try {
        reverseBinaryToASCII(binaryFilename, outputFilename);
        std::cout << "Binary file reversed to ASCII successfully." << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
