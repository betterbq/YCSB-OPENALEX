#include "FileKeyValueGenerator.h"

FileKeyValueGenerator::FileKeyValueGenerator(const std::string &directory) {
    DIR *dir = opendir(directory.c_str());
    if (dir == nullptr) {
        throw std::runtime_error("Could not open directory: " + directory);
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type == DT_REG) {
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

    return std::make_pair(key, value);
}

bool FileKeyValueGenerator::HasNext() const {
    return std::any_of(readFlags.begin(), readFlags.end(), [](bool read) { return !read; });
}
