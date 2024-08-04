# YCSB-C

Yahoo! Cloud Serving Benchmark in C++, a C++ version of YCSB (https://github.com/brianfrankcooper/YCSB/wiki)

## Quick Start

To build YCSB-C on Ubuntu, for example:

```
$ sudo apt-get install libtbb-dev
$ make
```

As the driver for Redis is linked by default, change the runtime library path
to include the hiredis library by:
```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
```

Run Workload A with a [TBB](https://www.threadingbuildingblocks.org)-based
implementation of the database, for example:
```
<<<<<<< HEAD
./ycsbc -db tbb_rand -threads 4 -P workloads/workloada.spec
=======
./ycsbc -db leveldb -filename /mnt/leveldb -P workloads/workloada.spec
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
```
Also reference run.sh and run\_redis.sh for the command line. See help by
invoking `./ycsbc` without any arguments.

Note that we do not have load and run commands as the original YCSB. Specify
how many records to load by the recordcount property. Reference properties
files in the workloads dir.

<<<<<<< HEAD
=======
## <u>Run the OPENALEX dataset</u>

### Get ready

```markdown
```sh
$ sudo apt-get install libjsoncpp-dev
$ make
```

### Download the OPENALEX Dataset

Please refer to [Download to your machine | OpenAlex technical documentation](https://docs.openalex.org/download-all-data/download-to-your-machine).

### Run OpenAlex

```sh
./ycsbc -db leveldb -filename /mnt/leveldb -P workloads/workloada.spec -OPENALEX /home/user/openalex
```

Please make sure to replace `/home/user/openalex` with the actual path to your OpenAlex data directory.
>>>>>>> 9a2d8c221191754e3f8abbfd0b37a96cc11bffa1
