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

void sortListByLength(vector<pair<string, vector<uint8_t>>>& invertedLists, const unordered_map<string, LexiconEntry>& lexicon) {
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

int searchBlockIndex(const vector<BlockMetaData>& blockMetaDataVec, const int& listStartPos) {
    int left = 0;
    int right = blockMetaDataVec.size() -1;
    int index = -1;

    while (left <=right) {
        int mid = (left + right) /2;
        if (blockMetaDataVec[mid].offset < listStartPos) {
            left = mid + 1;
        }
        else if (blockMetaDataVec[mid].offset > listStartPos){
            right = mid - 1;
        }
        else {
            index = mid;
            break;
        }
    }
    return index;
}

template <typename T>
vector<T> sliceVector(const vector<T>& vec, int start, int length) {
    return vector<T>(vec.begin() + start, vec.begin()+start+length);
}

int searchNextDocID(vector<int>docIDListBlock, int lookUpDocID) {
    int left = 0;
    int right = docIDListBlock.size();
    int foundIndex = -1;

    while (left <= right) {
        int mid = (left + right)/2;
        if (docIDListBlock[mid] < lookUpDocID) {
            left = mid + 1;
        }
        else {
            foundIndex = mid;
            right = mid - 1;
        }
    }
    return foundIndex;

}

pair<int,int> nextGEQ(const pair<string, vector<uint8_t>>& invertedList, 
            const int& lookUpDocID, const vector<BlockMetaData>& blockMetaDataVec, 
            const unordered_map<string, LexiconEntry>& lexiconMap) {
    //cout << "starting nextGEQ" << endl;
    string term = invertedList.first;
    //cout << term << endl;
    vector<uint8_t> list = invertedList.second;
    int listStartPos = lexiconMap.at(term).offset;
    //cout << "start position is " << listStartPos << endl;
    int listRestLength = lexiconMap.at(term).length;
    //cout << "length " << listRestLength << endl;
    int foundIndex = -1;
    int foundDocID;
    int foundFreq;

    int blockIndex = searchBlockIndex(blockMetaDataVec, listStartPos);
    //cout << "blockIndex is " << blockIndex << endl;
    //cout << "blockStart is " << blockMetaDataVec[blockIndex].offset << endl;
    //cout << "blockSize is " << blockMetaDataVec[blockIndex].length << endl;
    if (blockIndex == -1) {
        cerr << "error finding the next docID" << endl;
        return make_pair(-1,-1);
    }
    listStartPos = 0;
    bool startOfList = true;
    while (listRestLength >= 0) {
        if (blockMetaDataVec[blockIndex].lastDocID < lookUpDocID) {
            startOfList = false;
            //cout << "block last docID is " << blockMetaDataVec[blockIndex].lastDocID << endl;
            //cout << "pushing to next block" << endl;
            listRestLength -= blockMetaDataVec[blockIndex].length;
            listStartPos += blockMetaDataVec[blockIndex].length;
            blockIndex++;
        }
        else {
            //cout << "block last docID is " << blockMetaDataVec[blockIndex].lastDocID << endl;
            //cout << "stay at this block" << endl;
            vector<uint8_t> listBlock = sliceVector(list, listStartPos, blockMetaDataVec[blockIndex].length);
            vector<int> decompressedListBlock = bytesToIntVec(listBlock);
            /*for (const auto& number : decompressedListBlock) {
                cout << number << " ";
            }
            cout << endl;*/
            int listBlockSize = decompressedListBlock.size();
            vector<int> docIDListBlock = sliceVector(decompressedListBlock, 0, listBlockSize/2);
            
            int prevtDocID;
            if (startOfList) {
                prevtDocID = 0;
            } else {
                prevtDocID = blockMetaDataVec[blockIndex-1].lastDocID;
            }
            for (auto& docID : docIDListBlock)  {
                docID += prevtDocID;
                prevtDocID = docID;
            }
            /*for (const auto& number : docIDListBlock) {
                cout << number << " ";
            }*/
            if (docIDListBlock[docIDListBlock.size()-1] < lookUpDocID) {
                foundDocID = -1;
                foundFreq = -1;
                break;
            } else {
                foundIndex = searchNextDocID(docIDListBlock, lookUpDocID);
                foundDocID = docIDListBlock[foundIndex];
                foundFreq = decompressedListBlock[foundIndex + listBlockSize/2];
                break;
            }
            
            
        }
    }
    if (foundIndex == -1) {
        return make_pair(-1,-1);
    }
    else {
        return make_pair(foundDocID, foundFreq);
    }
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
            if (count > 400) {
                break;
            }
            count++;
        }*/
    

        cout << "search engine is ready" << endl;

        vector<string> query = {"peacefully"};
        //read in inverted index lists
        vector<pair<string, vector<uint8_t>>> invertedLists = readInvertedIndices(query, lexiconMap, indexFilePath);
        sortListByLength(invertedLists, lexiconMap);

        for (const auto& termList : invertedLists) {
            string term = termList.first;
            vector<int> numbers = bytesToIntVec(termList.second);
            /*for (const auto& number : numbers) {
                cout << number << " ";
            }
            cout << endl;*/
            pair<int,int> found = nextGEQ(termList, 3, blockMetaDataVec, lexiconMap);
            cout << "docID is " << found.first << endl;
            cout << "frequency is " << found.second << endl;;
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


