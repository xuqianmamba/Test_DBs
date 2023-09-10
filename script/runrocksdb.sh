#!/bin/bash
g++ ../src/testrocksdb.cpp -o ../bin/testrocksdb -std=c++17 ../libs/librocksdb.a -I ../libs/rocksdb/include/ -lpthread -ldl -lrt -lsnappy -lgflags -lz -lbz2 -lzstd -llz4 -lboost_program_options
../bin/testrocksdb --DB ../DB_path/rocksdb --options write

# strace -e trace=read,write,open,close ../bin/testrocksdb --DB ../DB_path/rocksdb  --csr-vlist /home/xuqian/benchmarks/twitter/twitter_vlist.txt --csr-elist /home/xuqian/benchmarks/twitter/twitter_elist.txt --options readseq > strace_output.txt 2>&1




# # 获取testlmdb的进程ID（PID）
# pid=$!

# # 每秒监视VmRSS并将其输出到文本文件
# while true; do
#     # 检查testlmdb是否仍在运行
#     if ! ps -p $pid > /dev/null; then
#         echo "Testlmdb has exited, stopping monitor script"
#         break
#     fi

#     timestamp=$(date "+%Y-%m-%d %H:%M:%S")
#     vmrss=$(ps -o pid=,rss= -p $pid | awk '{print $2}')
#     echo "$timestamp VmRSS: $vmrss kB" >> /home/xuqian/k-degree/rocksdb3.0.txt
#     sleep 0.001
# done