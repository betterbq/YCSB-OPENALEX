#include "FileKeyValueGenerator.h"
<<<<<<< HEAD

FileKeyValueGenerator::FileKeyValueGenerator(const std::string &directory) {
=======
#include <dirent.h>
#include <stdexcept>
#include <sstream>
#include <iostream>

FileKeyValueGenerator::FileKeyValueGenerator(const std::string &directory) : currentFileIndex(0), endOfFiles(false) {
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
    DIR *dir = opendir(directory.c_str());
    if (dir == nullptr) {
        throw std::runtime_error("Could not open directory: " + directory);
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type == DT_REG) {
<<<<<<< HEAD
            std::string filePath = directory + "/" + entry->d_name;
            BuildIndex(filePath);
        }
    }
    closedir(dir);
    if (index.empty()) {
        throw std::runtime_error("No files found in directory: " + directory);
    }

    // 初始化readFlags数组，初始值为false
    readFlags.resize(index.size(), false);
    writerBuilder["indentation"] = "";
}

FileKeyValueGenerator::~FileKeyValueGenerator() {}

void FileKeyValueGenerator::BuildIndex(const std::string &filePath) {
    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Couldn't open file: " + filePath);
    }
    std::string line;
    std::streampos pos = file.tellg();
    while (std::getline(file, line)) {
        index.emplace_back(filePath, pos);
        pos = file.tellg();
    }
    file.close();
}

std::pair<std::string, std::string> FileKeyValueGenerator::Next() {
    if (index.empty()) {
        throw std::runtime_error("No more key-value pairs to read");
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, index.size() - 1);

    size_t randomIndex;
    do {
        randomIndex = dis(gen);
    } while (readFlags[randomIndex]);

    readFlags[randomIndex] = true;

    std::pair<std::string, std::streampos> entry = index[randomIndex];
    std::string filePath = entry.first;
    std::streampos offset = entry.second;

    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Couldn't open file: " + filePath);
    }

    file.seekg(offset);
    std::string currentLine;
    std::getline(file, currentLine);
    file.close();

=======
            files.emplace_back(directory + "/" + entry->d_name);
        }
    }
    closedir(dir);
    if (files.empty()) {
        throw std::runtime_error("No files found in directory: " + directory);
    }
    writerBuilder["indentation"] = "";
    LoadNextFile();
}

FileKeyValueGenerator::~FileKeyValueGenerator() {
    if (currentFile.is_open()) {
        currentFile.close();
    }
}

void FileKeyValueGenerator::LoadNextFile() {
    if (currentFile.is_open()) {
        currentFile.close();
    }
    if (currentFileIndex < files.size()) {
        currentFile.open(files[currentFileIndex]);
        if (!currentFile.is_open()) {
            throw std::runtime_error("Couldn't open file: " + files[currentFileIndex]);
        }
        currentFileIndex++;
        LoadNextLine();
    } else {
        endOfFiles = true;
    }
}

void FileKeyValueGenerator::LoadNextLine() {
    if (!std::getline(currentFile, currentLine)) {
        LoadNextFile();
    }
}

std::pair<std::string, std::string> FileKeyValueGenerator::Next() {
    if (endOfFiles) {
        throw std::runtime_error("No more key-value pairs to read");
    }

>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
    Json::Value root;
    std::string errs;
    std::istringstream sstream(currentLine);

    if (!Json::parseFromStream(readerBuilder, sstream, &root, &errs)) {
        throw std::runtime_error("Error parsing JSON: " + errs);
    }

    if (!root.isMember("id")) {
        throw std::runtime_error("JSON does not contain 'id' field");
    }

    std::string key = root["id"].asString().substr(21);
    root.removeMember("id");
    std::string value = Json::writeString(writerBuilder, root);

<<<<<<< HEAD
=======
    LoadNextLine();

>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
    return std::make_pair(key, value);
}

bool FileKeyValueGenerator::HasNext() const {
<<<<<<< HEAD
    return std::any_of(readFlags.begin(), readFlags.end(), [](bool read) { return !read; });
}
=======
    return !endOfFiles;
}
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
