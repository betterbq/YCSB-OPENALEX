//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) { }
  virtual  bool DoInsertOpenalex(std::pair<std::string, std::string>& kv);
  virtual bool DoInsert();
  virtual int DoTransaction(int batch_num);
  
  virtual ~Client() { }
  
 protected:
  
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite(int batch_num);
  virtual int TransactionScan(int batch_num);
  virtual int TransactionUpdate(int batch_num);
  virtual int TransactionInsert(int batch_num);
  virtual int TransactionBatchRead(int batch_num);
  
  DB &db_;
  CoreWorkload &workload_;

};


inline bool Client::DoInsertOpenalex(std::pair<std::string, std::string>& kv) {
    std::vector<DB::KVPair> pairs = {kv};
    return (db_.Insert(workload_.NextTable(), kv.first, pairs) == DB::kOK);

}

inline bool Client::DoInsert() {
  std::string key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_.BuildValues(pairs);
  return (db_.Insert(workload_.NextTable(), key, pairs) == DB::kOK);
}

inline int Client::DoTransaction(int batch_num) {
  int status = -1;
  switch (workload_.NextOperation()) {
    case READ:
      status = TransactionRead();
      break;
    case UPDATE:
      status = TransactionUpdate(batch_num);
      break;
    case INSERT:
      status = TransactionInsert(batch_num);
      break;
    case SCAN:
      status = TransactionScan(batch_num);
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite(batch_num);
      break;
    case BATCHREAD:
      status = TransactionBatchRead(batch_num);
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return status/*(status == DB::kOK)*/;
}

inline int Client::TransactionRead() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(table, key, &fields, result);
  } else {
    return db_.Read(table, key, NULL, result);
  }
}
inline int Client::TransactionBatchRead(int batch_num){
	 const std::string &table = workload_.NextTable();
	 std::vector<std::string> fields;
	 std::vector<DB::KVPair> result;
	 //int surplus_read_num_for_batch=workload_.read_total_num_for_batch_-
		//	 workload_.done_read_num_for_batch_;
	 //int this_batch_num;
	 //if(surplus_read_num_for_batch>workload_.batch_read_num()){
	//	 this_batch_num=workload_.batch_read_num();
	 //}else{
	//	 this_batch_num=surplus_read_num_for_batch;
	// }
	 std::vector<std::string> keys;
	 for(int i=0;i<batch_num;i++){
		  std::string key = workload_.NextTransactionKey();
		  keys.push_back(key);
		//  workload_.done_read_num_for_batch_++;
	 }
	 return db_.BatchRead(table, keys, batch_num);
}
inline int Client::TransactionReadModifyWrite(int batch_num) {
  const std::string &table = workload_.NextTable();
    std::vector<std::string> keys;
    for(int i=0;i<batch_num;i++){
        std::string key = workload_.NextTransactionKey();
        keys.push_back(key);
    }
    db_.BatchRead(table, keys, batch_num);

    int count = 0;
    for(auto i : keys)
    {
        std::vector<DB::KVPair> values;
        if (workload_.write_all_fields()) {
            workload_.BuildValues(values);
        } else {
            workload_.BuildUpdate(values);
        }
        count+=((db_.Update(table, i, values))==DB::kOK);
    }
    return count;
/*  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }*/

 /* std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);*/
}

inline int Client::TransactionScan(int batch_num) {
  const std::string &table = workload_.NextTable();
  int count = 0;
  for(int i=0; i<batch_num; i++){
      int len = workload_.NextScanLength();
      std::string start_key;
      std::string end_key;
      workload_.NextTransactionScanKey(start_key,end_key,len);

      std::vector<std::vector<DB::KVPair>> result;
      if (!workload_.read_all_fields()) {
          std::vector<std::string> fields;
          fields.push_back("field" + workload_.NextFieldName());
          count+=(db_.Scan(table,start_key,end_key, len, &fields, result)==DB::kOK);
      } else {
          count+=(db_.Scan(table, start_key,end_key, len, NULL, result)==DB::kOK);
      }
  }
    return count;
 /* int len = workload_.NextScanLength();
  std::string start_key;
  std::string end_key;
  workload_.NextTransactionScanKey(start_key,end_key,len);

  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(table,start_key,end_key, len, &fields, result);
  } else {
    return db_.Scan(table, start_key,end_key, len, NULL, result);
  }*/
}

inline int Client::TransactionUpdate(int batch_num) {
  const std::string &table = workload_.NextTable();
  int count = 0;
  for(int i=0; i<batch_num; i++){
      const std::string &key = workload_.NextTransactionKey();
      std::vector<DB::KVPair> values;
      if (workload_.write_all_fields()) {
          workload_.BuildValues(values);
      } else {
          workload_.BuildUpdate(values);
      }
      count+=(db_.Update(table, key, values) == DB::kOK);
  }
  return count;
}

inline int Client::TransactionInsert(int batch_num) {
  const std::string &table = workload_.NextTable();
  int count = 0;
  for(int i=0; i<batch_num; i++){
      const std::string &key = workload_.NextSequenceKey();
      std::vector<DB::KVPair> values;
      workload_.BuildValues(values);
      count+=(db_.Insert(table, key, values)==DB::kOK);
  }
    return count;
/*  const std::string &key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  return db_.Insert(table, key, values);*/
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_
