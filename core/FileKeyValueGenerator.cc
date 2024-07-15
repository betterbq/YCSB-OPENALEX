#include "FileKeyValueGenerator.h"
#include <dirent.h>
#include <stdexcept>
#include <sstream>
#include <iostream>

FileKeyValueGenerator::FileKeyValueGenerator(const std::string &directory) : currentFileIndex(0), endOfFiles(false) {
    DIR *dir = opendir(directory.c_str());
    if (dir == nullptr) {
        throw std::runtime_error("Could not open directory: " + directory);
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type == DT_REG) {
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

    LoadNextLine();

    return std::make_pair(key, value);
}

bool FileKeyValueGenerator::HasNext() const {
    return !endOfFiles;
}