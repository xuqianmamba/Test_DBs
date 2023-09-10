#include "../include/testrocksdb.hpp"
#include <boost/program_options.hpp>

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help", "produce help message")
        ("DB,d", po::value<std::string>(), "path for DB")
        ("csr-elist,e", po::value<std::string>(), "path for csr_elist")
        ("csr-vlist,v", po::value<std::string>(), "path for csr_vlist")
        ("options,o", po::value<std::string>(), "options: write, readseq, readnonseq")
        ("value-size,s", po::value<int>()->default_value(0), "size of the value for write option")
        ("nonsequential,n",po::value<bool>(), "for random read/write")
    ;


    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 0;
    }

    if (vm.count("DB")) {
        std::cout << "Path for DB: " << vm["DB"].as<std::string>() << ".\n";
    } else {
        std::cerr << "Path for DB not specified.\n";
        return 1;
    }

    if (vm.count("csr-elist")) {
        std::cout << "Path for csr_elist: " << vm["csr-elist"].as<std::string>() << ".\n";
    } else {
        std::cout << "Path for csr_elist not specified.\n";
    }

    if (vm.count("csr-vlist")) {
        std::cout << "Path for csr_vlist: " << vm["csr-vlist"].as<std::string>() << ".\n";
    } else {
        std::cout << "Path for csr_vlist not specified.\n";
    }

    
    // init RocksDB
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    const std::string PATH  = vm["DB"].as<std::string>();
    vector<DTYPE> csr_vlist, csr_elist;
    // destory RocksDB
    rocksdb::DestroyDB(PATH, options);
    rocksdb::Status status = rocksdb::DB::Open(options, PATH, &db);
    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }

    if (vm.count("options")) {
        std::string option = vm["options"].as<std::string>();
        if (option == "write") {
            if (vm.count("value-size")) {
                int valueSize = vm["value-size"].as<int>();
                std::cout << "Write operation with value size: " << valueSize << ".\n";
            } else {
                std::cout << "Value size not specified for write option.Use defalut 0.\n";
            }

            std::vector<int> nums(10000000);
            for (int i = 0; i < 10000000; i++) {
                nums[i] = i;
            }

            //for random write 
            if (vm.count("nonsequential") && vm["nonsequential"].as<int>()==1) {
                unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
                std::shuffle(nums.begin(), nums.end(), std::default_random_engine(seed));
            }
            
            std::string value_str(vm["value-size"].as<int>(), 'X'); 

            // use WriteBatch
            const size_t MAX_BATCH_SIZE = 10 * 1024 * 1024; // 10MB
            rocksdb::WriteBatch batch;
            size_t batch_size = 0;

            auto start = std::chrono::high_resolution_clock::now();

            for (const int& i : nums) {
                std::string key = int_to_network_byte_order(i);
                batch.Put(key, value_str);
                batch_size += key.size() + value_str.size();

                if (batch_size >= MAX_BATCH_SIZE) {
                    status = db->Write(rocksdb::WriteOptions(), &batch);
                    if (!status.ok()) {
                        std::cerr << "Failed to batch write. Error: " << status.ToString() << std::endl;
                    }

                    batch.Clear();
                    batch_size = 0;
                }
            }

            // Write the remaining part
            if (batch_size > 0) {
                status = db->Write(rocksdb::WriteOptions(), &batch);
                if (!status.ok()) {
                    std::cerr << "Failed to batch write. Error: " << status.ToString() << std::endl;
                }
            }

            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << "Inserting 10000000 keys in random order using batches took " << duration.count() << " milliseconds" << std::endl;

            // Use an iterator to sequentially output all keys and their values, converting them to integers.

            // std::cout << "Keys and Values in RocksDB:" << std::endl;
            // rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
            // for (it->SeekToFirst(); it->Valid(); it->Next()) {
            //     std::string key_str = it->key().ToString();
            //     uint32_t net_value;
            //     std::memcpy(&net_value, key_str.data(), sizeof(uint32_t));
            //     int key_int = ntohl(net_value);

            //     std::cout << "Key: " << key_int << ", Value: " << it->value().ToString() << std::endl;
            // }

            // delete it;

        } else if (option == "readseq" || option == "readnonseq") {
            if (!vm.count("csr-elist") || !vm.count("csr-vlist")) {
                std::cerr << "csr-elist and csr-vlist paths are required for readseq and readnonseq options.\n";
                return 1;
            }
            // close Prefix Bloom Filter
            options.prefix_extractor.reset();
            // Enable statistics
            options.statistics = rocksdb::CreateDBStatistics();

            options.create_if_missing = true;
            rocksdb::BlockBasedTableOptions table_options;
            size_t cache_size=17179869183; //16G
            // size_t cache_size=8500000000; //8G

            table_options.block_cache = rocksdb::NewLRUCache(cache_size);  // 16GB
            table_options.cache_index_and_filter_blocks = true;
            options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

            Load_Vertex_File(vm["csr-elist"].as<std::string>(), csr_elist);
            Load_Vertex_File(vm["csr-vlist"].as<std::string>(), csr_vlist);

            Status status = DB::Open(options, PATH, &db);
            if (!status.ok()) {
                std::cerr << "Failed to open database: " << status.ToString() << std::endl;
                return 1;
            }

            cout<<"vlist_size: "<<csr_vlist.size()<<endl;
            cout<<"elist_size: "<<csr_elist.size()<<endl;

            // Collect and display statistics here.
            uint64_t cache_hits = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
            uint64_t cache_misses = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
            uint64_t bytes_read = options.statistics->getTickerCount(rocksdb::BYTES_READ);
            uint64_t bytes_written = options.statistics->getTickerCount(rocksdb::BYTES_WRITTEN);

            std::cout << "Cache Hits: " << cache_hits << std::endl;
            std::cout << "Cache Misses: " << cache_misses << std::endl;
            std::cout << "Bytes Read: " << bytes_read << " bytes" << std::endl;
            std::cout << "Bytes Written: " << bytes_written << " bytes" << std::endl;

            auto start = std::chrono::high_resolution_clock::now();
            Store_CSR(db, csr_vlist, csr_elist);
            auto end = std::chrono::high_resolution_clock::now();
            auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << "Storing CSR data took " << insert_duration.count() << " milliseconds" << std::endl;

            cache_hits = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
            cache_misses = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
            bytes_read = options.statistics->getTickerCount(rocksdb::BYTES_READ);
            bytes_written = options.statistics->getTickerCount(rocksdb::BYTES_WRITTEN);

            std::cout << "Cache Hits: " << cache_hits << std::endl;
            std::cout << "Cache Misses: " << cache_misses << std::endl;
            std::cout << "Bytes Read: " << bytes_read << " bytes" << std::endl;
            std::cout << "Bytes Written: " << bytes_written << " bytes" << std::endl;

            Status s = db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
            if (!s.ok()) {
                std::cerr << "Compaction failed: " << s.ToString() << std::endl;
            }
            if (option == "readseq"){
                //peform 3 times
                for (int round = 1; round <= 3; ++round) {
                    start = std::chrono::high_resolution_clock::now();
                    performCSRQueriesIter(db);
                    end = std::chrono::high_resolution_clock::now();
                    insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                    std::cout << "perform CSR Queries took " << insert_duration.count() << " milliseconds" << std::endl;

                    cache_hits = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
                    cache_misses = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
                    bytes_read = options.statistics->getTickerCount(rocksdb::BYTES_READ);
                    bytes_written = options.statistics->getTickerCount(rocksdb::BYTES_WRITTEN);

                    std::cout << "Cache Hits: " << cache_hits << std::endl;
                    std::cout << "Cache Misses: " << cache_misses << std::endl;
                    std::cout << "Bytes Read: " << bytes_read << " bytes" << std::endl;
                    std::cout << "Bytes Written: " << bytes_written << " bytes" << std::endl;

                    cout<<endl<<endl<<"**************************"<<endl<<endl;
                }
                
                std::cout << "Executing readseq operation.\n";
            } else {
                for (int round = 1; round <= 3; ++round) {
                    start = std::chrono::high_resolution_clock::now();
                    performCSRQueries(db);
                    end = std::chrono::high_resolution_clock::now();
                    insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                    std::cout << "perform CSR Queries took " << insert_duration.count() << " milliseconds" << std::endl;

                    cache_hits = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
                    cache_misses = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
                    bytes_read = options.statistics->getTickerCount(rocksdb::BYTES_READ);
                    bytes_written = options.statistics->getTickerCount(rocksdb::BYTES_WRITTEN);

                    std::cout << "Cache Hits: " << cache_hits << std::endl;
                    std::cout << "Cache Misses: " << cache_misses << std::endl;
                    std::cout << "Bytes Read: " << bytes_read << " bytes" << std::endl;
                    std::cout << "Bytes Written: " << bytes_written << " bytes" << std::endl;

                    cout<<endl<<endl<<"**************************"<<endl<<endl;
                }
                std::cout << "Executing readnonseq operation.\n";
            }
        } else {
            std::cerr << "Invalid option provided: " << option << ".\n";
            return 1;
        }
    }

    return 0;
}