#include <unordered_map>
#include <fstream>
#include <iostream>
#include <sstream>
#include <chrono>
#include <vector>
#include <algorithm>

using namespace std;

//struct definitions
struct LexiconEntry {
    uint64_t offset;     // Byte offset in the final index file
    uint32_t length;     // Number of bytes for this term's entry
    uint32_t docFreq;    // Number of documents containing the term
};

struct BlockMetaData {
    uint64_t offset;    // start position of the block in the index file
    uint32_t length;    // number of bytes of this block
    int lastDocID;  //lastdocid on this block
};

// Comparator struct for sorting inverted index list by the docFreq in lexicon of this term
struct ListLengthComparator {
    const unordered_map<string, LexiconEntry>& lexicon;

    // Constructor to initialize the lexicon reference
    ListLengthComparator(const unordered_map<string, LexiconEntry>& lex) : lexicon(lex) {}

    // Comparison operator
    bool operator()(const pair<string, vector<uint8_t>>& a,
                    const pair<string, vector<uint8_t>>& b) const {
        // Use the lexicon to get the length of list (in postings) for comparison
        return lexicon.at(a.first).docFreq < lexicon.at(b.first).docFreq;
    }
};

unordered_map<string, LexiconEntry> loadLexicon(const string& filePath) {
    unordered_map<string, LexiconEntry> lexiconMap;
    ifstream lexiconFile(filePath);

    if (!lexiconFile.is_open()) {
        throw runtime_error("Failed to open lexicon file for reading: " + filePath);
    }

    string line;
    while (getline(lexiconFile, line)) {
        istringstream iss(line);
        string term;
        uint64_t offset;
        uint32_t length; 
        uint32_t docFreq;

        if (iss >> term >> offset >> length >> docFreq) {
            lexiconMap[term] = {offset, length, docFreq};
        } else {
            cerr << "Error parsing line: " << line << endl;
        }
    }

    lexiconFile.close();
    cout << "lexicon file loaded" << endl;
    return lexiconMap;
}

unordered_map<int, int> loadPageTable(const string& filePath) {
    unordered_map<int, int> pageMap;
    ifstream pageTableFile(filePath);

    if (!pageTableFile.is_open()) {
        throw runtime_error("Failed to open page table file for reading: " + filePath);
    }

    string line;
    while (getline(pageTableFile, line)) {
        istringstream iss(line);
        int docID;
        int length; //length in words

        if (iss >> docID >> length) {
            pageMap[docID] = length;
        } else {
            cerr << "Error parsing line in page table file: " << line << endl;
        }
    }

    pageTableFile.close();
    cout << "page table file loaded" << endl;
    return pageMap;
}

vector<BlockMetaData> loadBlockMetaData(const string& filePath) {
    vector<BlockMetaData> blockMetaDataVec;
    ifstream metaDataFile(filePath);

    if (!metaDataFile.is_open()) {
        throw runtime_error("Failed to open block metadata file for reading: " + filePath);
    }

    uint32_t offset = 0;
    string line;
    while (getline(metaDataFile, line)) {
        istringstream iss(line);
        uint32_t length;
        int lastDocID;

        if (iss >> length >> lastDocID) {
            blockMetaDataVec.push_back({offset, length, lastDocID});
            offset += length;
        } else {
            cerr << "Error parsing line in page table file: " << line << endl;
        }
    }

    metaDataFile.close();
    cout << "block meta data file loaded" << endl;
    return blockMetaDataVec;
}

vector<uint8_t> openList(const string& term, const unordered_map<string, LexiconEntry> lexicon, ifstream& indexFile) {
    auto it = lexicon.find(term);
    //check if the term exists in the lexicon
    if (it == lexicon.end()) {
        cerr << "Term not found: " << term << endl;
        return {};
    }

    const LexiconEntry& entry = it->second;
    // seek to the start position for the term
    indexFile.seekg(entry.offset);
    // Read the specified number of bytes for the inverted index
    vector<uint8_t> buffer(entry.length);
    indexFile.read(reinterpret_cast<char*>(buffer.data()), entry.length);

    return buffer;

}

void sortListByLength(vector<pair<string, vector<uint8_t>>> invertedLists, const unordered_map<string, LexiconEntry>& lexicon) {
    sort(invertedLists.begin(), invertedLists.end(), ListLengthComparator(lexicon));
}

//function to turn bytes to int
int byteToInt(const vector<uint8_t>& bytes) {
    int value = 0;
    int shift = 0;

    for (size_t i = 0; i < bytes.size(); ++i) {
        value |= (bytes[i] & 0x7F) << shift; // Mask the highest bit and shift
        shift += 7;

        // If the highest bit is not set, we are done
        if (!(bytes[i] & 0x80)) {
            break;
        }
    }

    return value;
}

vector<int> bytesToIntVec(const vector<uint8_t>& bytes) {
    vector<int> numbers;
    vector<uint8_t> buffer;

    for (size_t i = 0; i < bytes.size(); ++i) {
        uint8_t byte = bytes[i];
        buffer.push_back(byte);

        // If the highest bit is not set, we have reached the end of the varbyte
        if (!(byte & 0x80)) {
            numbers.push_back(byteToInt(buffer)); // Convert to int and store
            buffer.clear(); // Clear buffer for the next varbyte
        }
    }
    
    // Handle case where buffer still has bytes (last varbyte)
    if (!buffer.empty()) {
        numbers.push_back(byteToInt(buffer));
    }

    return numbers;
}

vector<pair<string, vector<uint8_t>>> readInvertedIndices(const vector<string>& terms, const unordered_map<string, LexiconEntry>& lexicon, const string& filePath) {
    vector<pair<string, vector<uint8_t>>> invertedLists;
    ifstream indexFile(filePath, ios::binary);

    if (!indexFile.is_open()) {
        throw runtime_error("Failed to open inverted index file for reading: " + filePath);
    }

    for (const string& term : terms) {
        vector<uint8_t> list = openList(term, lexicon, indexFile);
        if (!list.empty()) {
            pair<string,vector<uint8_t>> invertedList(term, list);
            invertedLists.push_back(invertedList);
        }
    }
    indexFile.close();
    return invertedLists;

}

int main() {

    // Start the timer
    auto start = chrono::high_resolution_clock::now();
    string lexiconFilePath = "src/index_4/lexicon.txt";
    string pageTableFilePath = "src/pagetable.tsv";
    string blockMetaDataFilePath = "src/index_4/blockMetaData.txt";
    string indexFilePath = "src/index_4/index.bin";
    
    try {
        
        unordered_map<string, LexiconEntry> lexiconMap = loadLexicon(lexiconFilePath);

        /*int count = 0;

        for (const auto& entry : lexiconMap) {
            cout << entry.first << " " << entry.second.offset << " " << entry.second.length << " " << entry.second.docFreq << endl;
            if (count > 100) {
                break;
            }
            count++;
        }*/
        
        unordered_map<int, int> pageMap = loadPageTable(pageTableFilePath);

        /*int count = 0;

        for (const auto& entry : pageMap) {
            cout << entry.first << " " << entry.second << endl;
            if (count > 100) {
                break;
            }
            count++;
        }*/
        
        vector<BlockMetaData> blockMetaDataVec = loadBlockMetaData(blockMetaDataFilePath);

        /*int count = 0;

        for (const auto& entry : blockMetaDataVec) {
            cout << entry.offset << " " << entry.length << " " << entry.lastDocID << endl;
            if (count > 100) {
                break;
            }
            count++;
        }*/
    

        cout << "search engine is ready" << endl;

        vector<string> query = {"000000", "peacefully", "000000000000001"};
        //read in inverted index lists
        vector<pair<string, vector<uint8_t>>> invertedLists = readInvertedIndices(query, lexiconMap, indexFilePath);
        sortListByLength(invertedLists, lexiconMap);

        for (const auto& termList : invertedLists) {
            string term = termList.first;
            vector<int> numbers = bytesToIntVec(termList.second);
            for (const auto& number : numbers) {
                cout << number << " ";
            }
            cout << endl;
        }
    } catch (const exception& e) {
        cerr << e.what() << endl;
    }


    // Stop the timer
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> duration = end - start;
    cout << "Execution time: " << duration.count() << " seconds." << endl;
    
    return 0;

}


