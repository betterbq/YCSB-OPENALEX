#ifndef FILE_KEY_VALUE_GENERATOR_H
#define FILE_KEY_VALUE_GENERATOR_H

#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <random>
#include <vector>
#include <dirent.h>
#include <algorithm>
#include <jsoncpp/json/json.h>

class FileKeyValueGenerator {
public:
    FileKeyValueGenerator(const std::string &directory);
    ~FileKeyValueGenerator();
    std::pair<std::string, std::string> Next();
    bool HasNext() const;

private:
    void BuildIndex(const std::string &filePath);
    std::vector<std::pair<std::string, std::streampos>> index;
    std::vector<bool> readFlags;
    Json::StreamWriterBuilder writerBuilder;
    Json::CharReaderBuilder readerBuilder;
};

#endif // FILE_KEY_VALUE_GENERATOR_H
