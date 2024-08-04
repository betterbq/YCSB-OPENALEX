#ifndef FILE_KEY_VALUE_GENERATOR_H
#define FILE_KEY_VALUE_GENERATOR_H

<<<<<<< HEAD
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <random>
#include <vector>
#include <dirent.h>
#include <algorithm>
=======
#include <string>
#include <vector>
#include <utility>
#include <fstream>
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
#include <jsoncpp/json/json.h>

class FileKeyValueGenerator {
public:
    FileKeyValueGenerator(const std::string &directory);
    ~FileKeyValueGenerator();
    std::pair<std::string, std::string> Next();
    bool HasNext() const;

private:
<<<<<<< HEAD
    void BuildIndex(const std::string &filePath);
    std::vector<std::pair<std::string, std::streampos>> index;
    std::vector<bool> readFlags;
    Json::StreamWriterBuilder writerBuilder;
    Json::CharReaderBuilder readerBuilder;
};

#endif // FILE_KEY_VALUE_GENERATOR_H
=======
    void LoadNextFile();
    void LoadNextLine();

    std::vector<std::string> files;
    size_t currentFileIndex;
    std::ifstream currentFile;
    Json::CharReaderBuilder readerBuilder;
    Json::StreamWriterBuilder writerBuilder;
    std::string currentLine;
    bool endOfFiles;
};

#endif // FILE_KEY_VALUE_GENERATOR_H
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
