# Yahoo! Cloud System Benchmark
# Workload C: Read only
#   Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
#                        
#   Read/update ratio: 100/0
#  Default data size: 1KB records (2 fields, 500bytes each, plus key)
#   Request distribution: uniform

recordcount=40000000
operationcount=40000000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0
updateproportion=0.9972
scanproportion=0
insertproportion=0
batchreadproportion=0.0028
batchreadnum=40
readtotalnumforbatch=4480000
requestdistribution=zipfian
maxscanlength=1000

scanlengthdistribution=uniform



