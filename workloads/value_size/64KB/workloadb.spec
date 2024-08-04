# Yahoo! Cloud System Benchmark
# Workload B: Read mostly workload
#   Application example: photo tagging; add a tag is an update, but most operations are to read tags
#                        
#   Read/update ratio: 95/5
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=1562500
operationcount=1562500
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

fieldcount=1
fieldlength=65536

readproportion=0
updateproportion=0.5
scanproportion=0
insertproportion=0
batchreadproportion=0.5

requestdistribution=zipfian
