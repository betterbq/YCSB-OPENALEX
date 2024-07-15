//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include <string>
#include "db/basic_db.h"
#include "db/leveldb_db.h"
//#include "db/rocksdb_db.h"
//#include "db/leveldbVlog_db.h"
using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;
using ycsbc::LevelDB;
//using ycsbc::RocksDB;
//using ycsbc::LeveldbVlog;
DB* DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "basic") {
   // return new BasicDB;
  }
  else if (props["dbname"] == "leveldb"){
	return new LevelDB(props["dbfilename"].c_str(),props["configpath"].c_str(),props.KeyRanges());
  } //else if (props["dbname"] == "rocksdb"){
  //return new RocksDB(props["dbfilename"].c_str(),props["configpath"].c_str());
  //} else if (props["dbname"] == "leveldbVlog"){
    //return new LeveldbVlog(props["dbfilename"].c_str(),props["configpath"].c_str());
  //}
  else return NULL;
}

