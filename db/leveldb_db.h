#ifndef YCSB_C_LEVELDB_H
#define YCSB_C_LEVELDB_H
#include "core/db.h"
#include <iostream>
#include <string>
#include "core/properties.h"
#include <leveldb/db.h>
#include <leveldb/env.h>
#include<leveldb/filter_policy.h>
//#include<leveldb/statistics.h>
using std::cout;
using std::endl;

namespace ycsbc {
class LevelDB : public DB{
public :
    LevelDB(const char *dbfilename,const char* configPath,
    		std::vector<std::pair<int,std::pair<std::string, std::string>>> key_ranges);
    int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &start_key,const std::string& end_key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) ;

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) ;

  int Delete(const std::string &table, const std::string &key);
  int BatchRead(const std::string &table, const std::vector<std::string> &keys, int batch_num);
  void openStatistics();
  void printAccessFreq();
  void print_Nettask_takeon();
  virtual ~LevelDB();
  virtual void doSomeThing(const char *thing_str="adjust_filter");
  void Close();
private:
    leveldb::DB *db_;
    static bool hasRead;
    void printFilterCount();
};
}
#endif
