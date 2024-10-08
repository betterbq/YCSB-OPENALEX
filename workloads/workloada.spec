# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=40000000
operationcount=50000000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true
writeallfields=true

fieldcount=1
fieldlength=1000

readproportion=0
updateproportion=1
scanproportion=0
insertproportion=0

requestdistribution=zipfian

