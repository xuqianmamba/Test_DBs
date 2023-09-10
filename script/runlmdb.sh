# 运行testlmdb程序并在后台获取其进程ID（PID）

g++ ../src/testlmdb.cpp -o ../bin/testlmdb -std=c++17 ../libs/liblmdb.a -I ../libs/lmdb/libraries/liblmdb -lpthread -lboost_program_options
#for write
# ../bin/testlmdb --DB ../DB_path/lmdb --options write
../bin/testlmdb --DB ../DB_path/lmdb  --csr-vlist /home/xuqian/benchmarks/twitter/twitter_vlist.txt --csr-elist /home/xuqian/benchmarks/twitter/twitter_elist.txt --options readseq

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
#     echo "$timestamp VmRSS: $vmrss kB" >> /home/xuqian/k-degree/lmdb.txt
#     sleep 0.001
# done