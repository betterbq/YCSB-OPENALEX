#ifndef FILE_KEY_VALUE_GENERATOR_H
#define FILE_KEY_VALUE_GENERATOR_H

#include <string>
#include <vector>
#include <utility>
#include <fstream>
#include <jsoncpp/json/json.h>

class FileKeyValueGenerator {
public:
    FileKeyValueGenerator(const std::string &directory);
    ~FileKeyValueGenerator();
    std::pair<std::string, std::string> Next();
    bool HasNext() const;

private:
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