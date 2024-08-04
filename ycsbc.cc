//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"
#include <unistd.h>
<<<<<<< HEAD
#include <stdlib.h>
#include <climits>
=======
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
#include "core/FileKeyValueGenerator.h"

using namespace std;

bool load_openalex = false;
<<<<<<< HEAD
int load_nums = INT_MAX;
int read_nums = INT_MAX;
=======
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
string openalex_directory;

long long CalculatePercentile(const std::vector<long long>& latencies, double percentile) {
  if (latencies.empty()) return 0;

  size_t index = static_cast<size_t>(percentile * latencies.size());
  if (index >= latencies.size()) index = latencies.size() - 1;
  return latencies[index]; // 返回延迟值，单位为微秒
}


void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading) {
  db->Init();
  ycsbc::Client client(*db, *wl);

<<<<<<< HEAD
  if (load_openalex) {
      if (is_loading) {
          int count = 0;
          try {
              FileKeyValueGenerator fileGenerator(openalex_directory);
              while (count <= num_ops && fileGenerator.HasNext()) {
                  std::pair<std::string, std::string> kv = fileGenerator.Next();
                  count += client.DoInsertOpenalex(kv);
                  std::cout << kv.first << "\t" << count << std::endl;
              }
          } catch (const std::exception &ex) {
              std::cerr << "Error: " << ex.what() << std::endl;
              return 1;
          }
          return count;
      }
      else { // Read OpenAlex
          int count = 0;
          int batch_num = 100;
          FileKeyValueGenerator fileGenerator(openalex_directory);
          while (count <= num_ops && fileGenerator.HasNext()) {
              std::vector<std::string> keys;
              for (int i = 0; i < batch_num && count <= num_ops && fileGenerator.HasNext(); ++i) {
                  std::pair<std::string, std::string> kv = fileGenerator.Next();
                  keys.push_back(kv.first);
                  ++count; // 增加计数
              }
              if (!keys.empty()) {
                  int batch_processed = client.DoBatchReadOpenalex(keys, batch_num);
                  if (batch_processed < 0) {
                      std::cerr << "Batch read error" << std::endl;
                      return 1; // 错误码，指示读取失败
                  }
              }
          }
          return count;
      }
  }




=======
  if (load_openalex) { //makefile 记的改
      int count = 0;
      try {
          FileKeyValueGenerator fileGenerator(openalex_directory);
          while (fileGenerator.HasNext()) {
              std::pair<std::string, std::string> kv = fileGenerator.Next();
              count += client.DoInsertOpenalex(kv);
              std::cout << kv.first << "\t" << count << std::endl;
          }
      } catch (const std::exception &ex) {
          std::cerr << "Error: " << ex.what() << std::endl;
          return 1;
      }
      return count;
  }

>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
  int oks = 0;
  int batch_num = 120;
  int next_report_=100;
  std::vector<long long> latencies;
  latencies.reserve(num_ops);

  for (int i = 0; i < num_ops;) {////这里，选项  其中一个设置为DoKVInsert
      if (is_loading) {
      oks += client.DoInsert();
      i++;
    } else {
      auto start = std::chrono::high_resolution_clock::now();
      int count = client.DoTransaction(batch_num);
      auto end = std::chrono::high_resolution_clock::now();
      long long latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(); // 确保这里转换为微秒
      latencies.push_back(latency);

      oks += count;
      i+=count;
      //oks += 120;
      //i += 120;
    }
    //std::cout<<"111"<<std::endl;
      if ( oks >= next_report_) {
          if      (next_report_ < 1000)   next_report_ += 100;
          else if (next_report_ < 5000)   next_report_ += 500;
          else if (next_report_ < 10000)  next_report_ += 1000;
          else if (next_report_ < 50000)  next_report_ += 5000;
          else if (next_report_ < 100000) next_report_ += 10000;
          else if (next_report_ < 500000) next_report_ += 50000;
          else                            next_report_ += 100000;
          fprintf(stderr, "... finished %d ops%30s\r", oks, "");
          fflush(stderr);
      }
  }
    if(!is_loading){
    // 排序延迟以计算百分位数
    std::sort(latencies.begin(), latencies.end());

    // 计算百分位的延迟
    long long p75 = CalculatePercentile(latencies, 0.75);
    long long p90 = CalculatePercentile(latencies, 0.90);
    long long p99 = CalculatePercentile(latencies, 0.99);
    long long p999 = CalculatePercentile(latencies, 0.999);
    long long p9999 = CalculatePercentile(latencies, 0.9999);

    // 输出百分位的延迟除以 read_batch 的结果，取小数点后两位
    fprintf(stderr, "New 75百分位延迟: %.2f 微秒\n", static_cast<double>(p75)/batch_num);
    fprintf(stderr, "New 90百分位延迟: %.2f 微秒\n", static_cast<double>(p90)/batch_num);
    fprintf(stderr, "New 99百分位延迟: %.2f 微秒\n", static_cast<double>(p99)/batch_num);
    fprintf(stderr, "New 99.9百分位延迟: %.2f 微秒\n", static_cast<double>(p999)/batch_num);
    fprintf(stderr, "New 99.99百分位延迟: %.2f 微秒\n", static_cast<double>(p9999)/batch_num);
  }
  if(!is_loading)
	  db->print_Nettask_takeon();

  db->Close();
  return oks;
}

int main(const int argc, const char *argv[]) {
	 FILE *fp;
	 char buf[1024];
	 char cpu[5];
	 long int user,nice,sys,idle,iowait,irq,softirq;
	 long int all1,all2,idle1,idle2;
	 float usage;
	 fp = fopen("/proc/stat","r");
	 if(fp == NULL){
		 perror("fopen");
		 exit(0);
	 }
	 fgets(buf, sizeof(buf), fp);
#if _DEBUG_
	 printf("buf=%s", buf);
#endif
	 sscanf(buf,"%s%ld%ld%ld%ld%ld%ld%ld", cpu, &user, &nice, &sys, &idle, &iowait, &irq, &softirq);//不用担心数据量过大的问题，再大也能解析出来
	 all1=user+nice+sys+idle+iowait+irq+softirq;
	 idle1=idle;
	 rewind(fp);

  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);///如果是openalex，   bool load_openalex = true; openalex_directory;

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  std::cout<<"-------------------------Load OPENALEX-------------------------"<<std::endl;
  if (load_openalex == true) {
<<<<<<< HEAD
      int total_ops = load_nums;
=======
      int total_ops = 0;
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
      utils::Timer<double> time_openalex;
      vector<future<int>> actual_ops;
      time_openalex.Start();
      for (int i = 0; i < num_threads; ++i) {//total_ops is does not matter
          actual_ops.emplace_back(async(launch::async,
                                        DelegateClient, db, &wl, total_ops / num_threads, true));
      }
      assert((int)actual_ops.size() == num_threads);
      int sum = 0;
      for (auto &n : actual_ops) {
          assert(n.valid());
          sum += n.get();
      }
      double duration_load = time_openalex.End();
      cerr <<endl<< "# Transaction throughput (KTPS)" << endl;
      cerr << sum / duration_load / 1000 <<endl;
<<<<<<< HEAD

      std::cout<<"-------------------------READ OPENALEX-------------------------"<<std::endl;

      int total_ops_read = read_nums;
      utils::Timer<double> time_openalex_read;
      vector<future<int>> actual_ops_read;
      time_openalex_read.Start();
      for (int i = 0; i < num_threads; ++i) {//total_ops is does not matter
          actual_ops_read.emplace_back(async(launch::async,
                                        DelegateClient, db, &wl, total_ops_read / num_threads, false));
      }
      assert((int)actual_ops_read.size() == num_threads);
      int sum_read = 0;
      for (auto &n : actual_ops_read) {
          assert(n.valid());
          sum_read += n.get();
      }
      double duration_load_read = time_openalex_read.End();
      cerr <<endl<< "# Transaction throughput (KTPS)" << endl;
      cerr << sum_read / duration_load_read / 1000 <<endl;

      std::cout<<"-------------------------About OPENALEX Result-------------------------"<<std::endl;
      std::cout<<"LOAD OpenAlex:"<< std::endl << "Load nums: "<< sum <<std::endl;
      cerr <<endl<< "# LOAD OpenAlex throughput (KTPS)" << endl;
      cerr << sum / duration_load / 1000 <<endl;
      std::cout<<"READ OpenAlex:"<< std::endl << "READ nums: "<< sum_read <<std::endl;
      cerr <<endl<< "# READ OpenAlex throughput (KTPS)" << endl;
      cerr << sum_read / duration_load_read / 1000 <<endl;
      std::cout<<"-------------------------Finish OPENALEX Result-------------------------"<<std::endl;


=======
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
  } else {// load_openalex != true
      std::cout<<"-------------------------Load YCSB-------------------------"<<std::endl;
      // Loads data
      vector<future<int>> actual_ops;
      int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
      utils::Timer<double> timer_load;
      timer_load.Start();
      for (int i = 0; i < num_threads; ++i) {
          actual_ops.emplace_back(async(launch::async,
<<<<<<< HEAD
                                        DelegateClient, db, &wl, total_ops / num_threads, false));
=======
                                        DelegateClient, db, &wl, total_ops / num_threads, true));
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
      }
      assert((int)actual_ops.size() == num_threads);

      int sum = 0;
      for (auto &n : actual_ops) {
          assert(n.valid());
          sum += n.get();
      }
      double duration_load = timer_load.End();

      std::cout<<"-------------------------Transaction-------------------------"<<std::endl;

      // Peforms transactions
      actual_ops.clear();
      total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
      utils::Timer<double> timer;
      timer.Start();
      for (int i = 0; i < num_threads; ++i) {
          actual_ops.emplace_back(async(launch::async,
                                        DelegateClient, db, &wl, total_ops / num_threads, false));
      }
      assert((int)actual_ops.size() == num_threads);

      sum = 0;
      for (auto &n : actual_ops) {
          assert(n.valid());
          sum += n.get();
      }
      //db->print_Nettask_takeon();
      double duration = timer.End();
      cerr << "# Loading records:\t" << sum << endl;
      cerr << "load duration:\t" << duration_load << endl;
      cerr << "# Transaction throughput (KTPS)" << endl;
      cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
      double batchread_proportion=wl.BatchReadWeight();
      /*if(batchread_proportion>0){
           int batchread_ops=total_ops*batchread_proportion;
           int other_ops=total_ops-batchread_ops;
           total_ops=other_ops+wl.read_total_num_for_batch_;
       }*/
      cerr << total_ops / duration / 1000 << endl;
      cerr << "performs transactions duration:\t" << duration << endl;
      cerr << "# Transaction update latency(ms):" <<duration*1000/total_ops<< endl;



  }// else load_openalex != true






  std::string cmd = "cat /proc/";
  int pid_ = getpid();
  char connectString_[10];
  sprintf(connectString_, "%d", pid_);
  std::string cmd_result = cmd + connectString_ + "/io";
  const char *result = cmd_result.c_str();
  system(result);

  memset(buf,0,sizeof(buf));
  cpu[0]='\0';
  user=nice=sys=idle=iowait=irq=softirq=0;
  fgets(buf,sizeof(buf),fp);
#if _DEGUG_
  printf("buf=%s", buf);
#endif
  sscanf(buf,"%s%ld%ld%ld%ld%ld%ld%ld", cpu, &user, &nice, &sys, &idle, &iowait, &irq, &softirq);//不用担心数据量过大的问题，再大也能解析出来
  all2=user+nice+sys+idle+iowait+irq+softirq;
  idle2=idle;
  usage = all2-all1;
  usage = (usage-(idle2-idle1))/usage;
  printf("--------------------我自己的cpu使用率计算方法---------------------------");
  printf("all=%ld\n",all2-all1);
  printf("idle=%ld\n",all2-all1-(idle2-idle1));
  printf("cpu use = %.2f\%\n",usage);
  printf("--------------------我自己的cpu使用率计算方法---------------------------");
  fclose(fp);
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;


  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-filename") == 0) {
        argindex++;
        if (argindex >= argc) {
          UsageMessage(argv[0]);
          exit(0);
        }
        props.SetProperty("dbfilename", argv[argindex]);
        argindex++;
     }else if(strcmp(argv[argindex], "-KeyRanges") == 0){
      	argindex++;
         if (argindex >= argc) {
           UsageMessage(argv[0]);
           exit(0);
         }
     	char s[2000];
     	 std::vector<std::pair<int,std::pair<std::string, std::string>>> key_ranges;
     	const char* tmp=argv[argindex];
     	char n_char[3];
     	int n;
     	char junk;
     	if(sscanf(tmp,"%[^/]", n_char) == 1){
     		strcpy(s,tmp+strlen(n_char)+1);
     		n=atoi(n_char);
 			for(int i=0;i<n;i++){
 				char dev_id_char[3];
 				char start_userkey[20];
 				char end_userkey[20];
 				if(i==n-1)
 					sscanf(s,"%[^:]:%[^:]:%[^.].%c",dev_id_char,start_userkey,end_userkey,&junk);
 				else
 					sscanf(s,"%[^:]:%[^:]:%[^,],%s",dev_id_char,start_userkey,end_userkey,s);
 				std::string start_str(start_userkey);
 				std::string end_str(end_userkey);
 				int dev_id=atoi(dev_id_char);
 				key_ranges.push_back(std::make_pair(dev_id,std::make_pair(start_str,end_str)));
 			}
     	}
     	props.SetKeyRanges(key_ranges);
    	argindex++;

     }
    else if (strcmp(argv[argindex], "-configpath") == 0) {
         argindex++;
         if (argindex >= argc) {
           UsageMessage(argv[0]);
           exit(0);
         }
         props.SetProperty("configpath", argv[argindex]);
         argindex++;
       }
    else if (strcmp(argv[argindex], "-OPENALEX") == 0) {
        argindex++;
        if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
        }
        load_openalex = true;
        openalex_directory.assign(argv[argindex]);
        if (openalex_directory.back() != '/') openalex_directory += '/';
        argindex++;
    }
<<<<<<< HEAD
    else if (strcmp(argv[argindex], "load_nums") == 0) {
        argindex++;
        if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
        }
        load_nums = stoi(argv[argindex]);
        argindex++;
    }
    else if (strcmp(argv[argindex], "read_nums") == 0) {
        argindex++;
        if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
        }
        read_nums = stoi(argv[argindex]);
        argindex++;
    }
=======
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1

    else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }
  if (load_openalex) {
      return openalex_directory;
  }
  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "  -OPENALEX: load KV from the given files of directory. Don't need -P"<< endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

