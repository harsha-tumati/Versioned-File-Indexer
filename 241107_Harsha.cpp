#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <chrono>
#include <cstring>
#include <cctype>
#include <queue>

using namespace std;

// Base class
class Reader {
public:
    virtual ~Reader() = default;// For closing the file once the class is out of scope
    virtual size_t readChunk(char* buf, size_t bufSize) = 0;// Reads bufSize bytes and insert into buf
    virtual bool isEOF() const = 0;// For checking whether this EOF is reached
    virtual void reset() = 0;
};

// Opens file if it isn't open and reads files using buffered chunks
class BufferedFileReader : public Reader {
private:
    ifstream file_;
    bool eof_;

public:
    // Constructor
    explicit BufferedFileReader(const string& path) {
        eof_ = false;
        file_.open(path, ios::binary);
        if(!file_.is_open()){
            throw runtime_error("Cannot open file: " + path);
        }
    }

    // Destructor
    ~BufferedFileReader() override {
        if(file_.is_open()){
            file_.close();// Close the file when class goes out of scope 
        }
    }

    size_t readChunk(char* buf, size_t bufSize) {
        if(eof_){
            return 0; // Aleady at EOF
        }
        file_.read(buf, static_cast<streamsize>(bufSize)); // Trying to read bufSize bytes from the file into buffer
        size_t got = static_cast<size_t>(file_.gcount()); // Number of bytes actually read

        // If bytes read less than bufSize, we have reached EOF
        if(got<bufSize){
            eof_ = true; 
        } 
        return got;
    }

    bool isEOF() const override { 
        return eof_;
    }

    void reset() override {
        file_.clear();
        file_.seekg(0, ios::beg);
        eof_ = false;
    }

};



// Extracts alphanumeric words from a byte stream , converts it to lowercase and handles words split across buffer boundaries using carry_
class AlphanumTokenizer {
private:
    string carry_;// Stores partial token from previous chunk if a word spans across adjacent buffers
    static string toLower(string s){
        for(char& c : s){
            c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
        }
        return s;
    }

public:
    void extractTokens(const char* data, size_t len,vector<string>& out) {
        string chunk = carry_ + string(data, len); // Attach carry_ at the start of the chunk and then continue extracting words
        carry_.clear();
        size_t i = 0;
        while(i < chunk.size()){
            if(isalnum(static_cast<unsigned char>(chunk[i]))){
                size_t start = i;
                while(i<chunk.size() && isalnum(static_cast<unsigned char>(chunk[i]))){
                    ++i; // Continue scanning the current alphanumeric word
                }
                if(i == chunk.size()){
                    carry_ = chunk.substr(start);// Word continues into next chunk, store partial token
                } 
                else {
                    out.push_back(toLower(chunk.substr(start, i-start)));// A complete word in lowercase is inserted into vector 'out'
                }
            } 
            else{
                ++i;
            }
        }
    }

    // Emit any remaining token(carry_) after processing the final chunk
    void flushout(vector<string>& out){
        if(!carry_.empty()){
            out.push_back(toLower(carry_));
            carry_.clear();
        }
    }

    void resetState(){ 
        carry_.clear();
    }

};

template <typename K,typename V>
class VersionedIndex{
public:
    using FreqMap = unordered_map<K,V>;

    void add(const string& version,const K& key){
        indices_[version][key] += 1;
    }

    V query(const string& version,const K& key) const{
        auto vit = indices_.find(version);
        if(vit == indices_.end()){
            return V{};
        }
        auto kit = vit->second.find(key);
        if(kit == vit->second.end()){
            return V{};
        }
        return kit->second;
    }

    V query(const string& v1,const string& v2,const K& key) const{
        return query(v1,key) - query(v2,key);
    }

    const FreqMap& getMap(const string& version) const{
        auto it = indices_.find(version);
        if (it == indices_.end()) {
            throw runtime_error("Version not found: " + version);
        }
        return it->second;
    }

    bool hasVersion(const string& version) const{
        return indices_.count(version) > 0;
    }

private:
    unordered_map<string, FreqMap> indices_;
};

using WordIndex = VersionedIndex<string, long>;

class Query {
public:
    virtual ~Query() = default;
    virtual void execute() const = 0;
};

class WordQuery : public Query {
private:
    const WordIndex& idx_;
    string version_;
    string word_;
    static string toLower(string s) {
        for (char& c : s)
            c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
        return s;
    }

public:
    WordQuery(const WordIndex& idx,const string& version,const string& word)
        : idx_(idx), version_(version), word_(word){}

    void execute() const override{
        if(!idx_.hasVersion(version_)){
            throw runtime_error("Version not found: " + version_);
        }
        
        string w = toLower(word_);// Convert to lower case
        long freq = idx_.query(version_, w);
        cout << "Word Query Result\n";
        cout << "  Version : " << version_ << "\n";
        cout << "  Word    : " << word_ << "\n";
        cout << "  Count   : " << freq << "\n";
    }
};

class TopQuery : public Query {
private:
    const WordIndex& idx_;
    string version_;
    int k_;

public:
    TopQuery(const WordIndex& idx,const string& version,int k)
        : idx_(idx), version_(version), k_(k) {}

    void execute() const override {
        if(!idx_.hasVersion(version_)){
            throw runtime_error("Version not found: " + version_);
        }

        const auto& fm = idx_.getMap(version_);

        // Lambda Function for identifying the priority for the following priority queue
        auto comp = [](const pair<string, long>& a, const pair<string, long>& b){
            if(a.second != b.second){
                return a.second > b.second; // Lower frequency stays at top
            }
            return a.first < b.first;       // Alphabetically later stays at top
        };

        // Store only k-elements in the minHeap and if the count increases more than k then remove smallest element
        priority_queue<pair<string, long>, vector<pair<string, long>>, decltype(comp)> minHeap(comp);
        for (const auto& entry : fm) {
            minHeap.push(entry);
            if (minHeap.size() > static_cast<size_t>(k_)) {
                minHeap.pop();// remove smallest element in the queue(For the heap to store k largest elements)
            }
        }

        // Extract from heap to insert into an array
        vector<pair<string, long>> result;
        while (!minHeap.empty()) {
            result.push_back(minHeap.top());
            minHeap.pop();
        }

        cout << "Top-" << k_ << " Query Result\n";
        // We will print the array in reverse as it sorted in increasing order (since the heap gives us smallest first)
        for (size_t i = 0; i < result.size(); ++i) {
            cout << "  " << (i + 1) << ". " << result[result.size()-1-i].first << " -> " << result[result.size()-1-i].second << "\n";
        }
    }
};

class DiffQuery : public Query {
private:
    const WordIndex& idx_;
    string v1_;
    string v2_;
    string word_;
    static string toLower(string s) {
        for (char& c : s)
            c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
        return s;
    }

public:
    DiffQuery(const WordIndex& idx,const string& v1,const string& v2,const string& word)
        : idx_(idx), v1_(v1), v2_(v2), word_(word) {}

    void execute() const override {
        if (!idx_.hasVersion(v1_)) {
            throw runtime_error("Version not found: " + v1_);
        }
        if (!idx_.hasVersion(v2_)) {
            throw runtime_error("Version not found: " + v2_);
        }

        string w = toLower(word_);// Convert to lower case
        long diff = idx_.query(v1_, v2_, w);
        cout << "Diff Query Result\n";
        cout << "  Version1 : " << v1_ << "\n";
        cout << "  Version2 : " << v2_ << "\n";
        cout << "  Word     : " << word_ << "\n";
        cout << "  Count in " << v1_ << " : " << idx_.query(v1_, w) << "\n";
        cout << "  Count in " << v2_ << " : " << idx_.query(v2_, w) << "\n";
        cout << "  Difference (" << v1_ << " - " << v2_ << ") : " << diff << "\n";
    }
};

void indexFile(Reader& reader,AlphanumTokenizer& tokenizer,WordIndex& index,const string& version,size_t bufSize){
    vector<char> buf(bufSize);
    vector<string> tokens;

    reader.reset();
    tokenizer.resetState();

    while(true){
        size_t got = reader.readChunk(buf.data(),bufSize);
        if (got == 0) {
            break;
        }
        tokens.clear();
        tokenizer.extractTokens(buf.data(),got,tokens);
        for (const auto& tok : tokens){
            index.add(version,tok);
        }
    }

    tokens.clear();
    tokenizer.flushout(tokens);
    for (const auto& tok : tokens){
        index.add(version, tok);
    }
}

// Struct for storing all the args from command line
struct Args {
    string file1, file2;
    string version1, version2;
    size_t bufferKB = 512;
    string query;
    string word;
    int top = 10;
};

// Returns the value associated with a CLI flag 
static string getArg(int argc, char* argv[], const string& flag) {
    for (int i = 1; i + 1 < argc; ++i) {
        if (string(argv[i]) == flag) {
            return argv[i + 1]; // flag is found.
        }
    }
    return ""; // If the flag is not present, returns an empty string
}

// Parse command line arguments into the Args struct and perform basic validation
Args parse(int argc, char* argv[]) {
    Args a;

    string f  = getArg(argc, argv, "--file");
    string v  = getArg(argc, argv, "--version");
    a.file1    = getArg(argc, argv, "--file1");
    a.file2    = getArg(argc, argv, "--file2");
    a.version1 = getArg(argc, argv, "--version1");
    a.version2 = getArg(argc, argv, "--version2");

    if (a.file1.empty()) {
        if(f.empty()){
            throw invalid_argument("Missing input file: provide --file or --file1");
        }
        else{
            a.file1 = f; // Command doesn't include '--file1' but includes '--file', then insert this as an argument for '--file1'
        }
    }
    if (a.version1.empty()) {
        if(v.empty()){
            throw invalid_argument("Missing version: provide --version or --version1");
        }
        else{
            a.version1 = v; // Command doesn't include '--version1' but includes '--version', then insert this as an argument for '--version1'
        }
    }

    string bufStr = getArg(argc, argv, "--buffer");
    if (!bufStr.empty()) {
        try {
            int kb = stoi(bufStr);
            if (kb < 256 || kb > 1024) {
                throw invalid_argument("Buffer size must be between 256 and 1024 KB");
            }
            a.bufferKB = static_cast<size_t>(kb);
        }
        catch (...) {
            throw invalid_argument("--buffer must be a number between 256 and 1024");// If --buffer is present but the argument next to it is not a number
        }
    }

    a.query   = getArg(argc, argv, "--query");
    a.word    = getArg(argc, argv, "--word");
    string topStr = getArg(argc, argv, "--top");
    if (!topStr.empty()) { // '--top' found
        try {
            a.top = stoi(topStr);
            if (a.top <= 0) {
                throw invalid_argument("--top must be positive");
            }
        }
        catch (...) {
            throw invalid_argument("--top must be a valid integer");// If --top is present but the argument next to it is not a number
        } 
    }

    if (a.query.empty()) {
        throw invalid_argument("--query is required (word | top | diff)"); // '--query' is not present in command line
    }

    return a;
}

int main(int argc, char* argv[]) {
    auto startTime = chrono::high_resolution_clock::now();
    try {
        Args args = parse(argc,argv); // Parse args
        size_t bufSize = args.bufferKB * 1024; // Convert buffer size from KB to bytes
        WordIndex index;
        if(args.query == "diff"){
            if (args.file1.empty() || args.file2.empty()) {
                throw invalid_argument("diff query requires --file1 and --file2");
            }
            if (args.version1.empty() || args.version2.empty()) {
                throw invalid_argument("diff query requires --version1 and --version2");
            }
            {
                BufferedFileReader reader1(args.file1);
                AlphanumTokenizer  tok1;
                indexFile(reader1,tok1,index,args.version1,bufSize);
            }
            {
                BufferedFileReader reader2(args.file2);
                AlphanumTokenizer tok2;
                indexFile(reader2,tok2,index,args.version2,bufSize);
            }
        }
        else{
            if(args.file1.empty()){
                throw invalid_argument("--file is required");
            }
            if(args.version1.empty()){
                throw invalid_argument("--version is required");
            }
            BufferedFileReader reader(args.file1);
            AlphanumTokenizer tok;
            indexFile(reader,tok,index,args.version1,bufSize);
        }
        Query* q = nullptr;
        if(args.query == "word"){
            if (args.word.empty()) {
                throw invalid_argument("--word is required for word query");
            }
            q = new WordQuery(index, args.version1, args.word);
        }
        else if(args.query == "top"){
            q = new TopQuery(index, args.version1, args.top);
        }
        else if(args.query == "diff"){
            if (args.word.empty()) {
                throw invalid_argument("--word is required for diff query");
            }
            q = new DiffQuery(index, args.version1, args.version2, args.word);
        }
        else{
            throw invalid_argument("Unknown query type: " + args.query);
        }

        q->execute();
        delete q;

        auto endTime  = chrono::high_resolution_clock::now();
        double elapsed = chrono::duration<double>(endTime - startTime).count();
        cout << "\n--- Execution Info ---\n";
        if(args.query == "diff"){
            cout << "  Versions        : " << args.version1 << ", " << args.version2 << "\n";
        }
        else{
            cout << "  Version         : " << args.version1 << "\n";
        }
        cout << "  Buffer size     : " << args.bufferKB << " KB\n";
        cout << "  Execution time  : " << elapsed << " s\n";

    } catch (const exception& ex) {
        cerr << "[ERROR] " << ex.what() << "\n";
        cerr << "\nUsage examples:\n"
                  << "  Word query:\n"
                  << "    ./analyzer --file dataset_v1.txt --version v1 "
                     "--buffer 512 --query word --word error\n\n"
                  << "  Top-K query:\n"
                  << "    ./analyzer --file dataset_v1.txt --version v1 "
                     "--buffer 512 --query top --top 10\n\n"
                  << "  Diff query:\n"
                  << "    ./analyzer --file1 dataset_v1.txt --version1 v1 "
                     "--file2 dataset_v2.txt --version2 v2 "
                     "--buffer 512 --query diff --word error\n";
        return 1;
    }
    return 0;
}
