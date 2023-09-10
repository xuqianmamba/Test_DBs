#include "../include/testlmdb.hpp"
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

    MDB_env* env;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn* txn;
    const std::string PATH  = vm["DB"].as<std::string>();
    vector<DTYPE> csr_vlist, csr_elist;
    // Open LMDB environment
    int result = mdb_env_create(&env);
    if (result != MDB_SUCCESS) {
        std::cerr << "Error creating LMDB environment: " << mdb_strerror(result) << std::endl;
        return 1;
    }
    result = mdb_env_set_mapsize(env, 10000000000);//
    if (result != MDB_SUCCESS) {
        std::cerr << "Failed to set LMDB mapsize: " << mdb_strerror(result) << std::endl;
        mdb_env_close(env);
    }
    result = mdb_env_open(env, PATH.c_str(), MDB_WRITEMAP | MDB_MAPASYNC | MDB_NOSYNC | MDB_NOMETASYNC, 0664);
    if (result != MDB_SUCCESS) {
        std::cerr << "Error opening LMDB environment: " << mdb_strerror(result) << std::endl;
        mdb_env_close(env);
        return 1;
    }

    
    mdb_txn_begin(env, NULL, 0, &txn);
    mdb_open(txn, NULL, 0, &dbi);
    mdb_drop(txn, dbi, 0); // clear
    mdb_txn_commit(txn);

    MDB_stat mst;
    mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    mdb_dbi_open(txn, NULL, 0, &dbi);
    mdb_stat(txn, dbi, &mst);
    std::cout << "Number of records in the database: " << mst.ms_entries << std::endl;
    mdb_txn_abort(txn);


    if (vm.count("options")) {
        std::string option = vm["options"].as<std::string>();
        if (option == "write") {
            if (vm.count("value-size")) {
                int valueSize = vm["value-size"].as<int>();
                std::cout << "Write operation with value size: " << valueSize << ".\n";
            } else {
                std::cout << "Value size not specified for write option.Use defalut 0.\n";
            }

            std::string value_str(24, 'X'); // value

            auto start = std::chrono::high_resolution_clock::now();

            mdb_txn_begin(env, NULL, 0, &txn);  


            for (int i = 0; i < 10000000; i++) {
                std::string key_str = int_to_network_byte_order(i);

                key.mv_size = key_str.size();
                key.mv_data = (void *)key_str.data();
                data.mv_size = value_str.size();
                data.mv_data = (void *)value_str.data();

                mdb_put(txn, dbi, &key, &data, MDB_APPEND);
            }


            for (int i = 0; i < 10000000; i++) {
                std::string key_str = int_to_network_byte_order(i);

                key.mv_size = key_str.size();
                key.mv_data = (void *)key_str.data();

                if(vm["value-size"].as<int>()!=0){
                    data.mv_size = value_str.size();  
                    data.mv_data = (void *)value_str.data();  
                }
                
                else{
                    data.mv_size = 0;  
                    data.mv_data = (void *)nullptr;  
                }
                int put_result = mdb_put(txn, dbi, &key, &data, MDB_APPEND);
            }

            mdb_txn_commit(txn);  

            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << "Inserting 10000000 keys using LMDB took " << duration.count() << " milliseconds" << std::endl;

            // Use a cursor to output all keys and their values.

            // std::cout << "Keys and Values in LMDB:" << std::endl;
            // mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
            // MDB_cursor *cursor;
            // mdb_cursor_open(txn, dbi, &cursor);

            // while (mdb_cursor_get(cursor, &key, &data, MDB_NEXT) == 0) {
            //     std::string key_str(static_cast<char*>(key.mv_data), key.mv_size);
            //     uint32_t net_value;
            //     std::memcpy(&net_value, key_str.data(), sizeof(uint32_t));
            //     int key_int = ntohl(net_value);

            //     std::cout << "Key: " << key_int << ", Value: " << std::string(static_cast<char*>(data.mv_data), data.mv_size) << std::endl;
            // }

            // mdb_cursor_close(cursor);
            // mdb_txn_abort(txn);
        }
        
        else if (option == "readseq" || option == "readnonseq") {
            if (!vm.count("csr-elist") || !vm.count("csr-vlist")) {
                std::cerr << "csr-elist and csr-vlist paths are required for readseq and readnonseq options.\n";
                return 1;
            }
            Load_Vertex_File(vm["csr-elist"].as<std::string>(), csr_elist);
            Load_Vertex_File(vm["csr-vlist"].as<std::string>(), csr_vlist);
            cout<<"csr_vlist.size() is"<<csr_vlist.size()<<endl;
            cout<<"csr_elist.size() is"<<csr_elist.size()<<endl;
            cout << "LMDB environment initialized." << endl;

            auto start = std::chrono::high_resolution_clock::now();
            Store_CSR(env, dbi, csr_vlist, csr_elist);
            auto end = std::chrono::high_resolution_clock::now();
            auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << "Store CSR took " << insert_duration.count() << " milliseconds" << std::endl;
            
            if (option == "readseq"){
                for (int i = 0; i < 3; ++i) {  // Run the tests 3 times
                    cout << "Starting test round " << i + 1 << endl;
                    

                    start = std::chrono::high_resolution_clock::now();

                    performCSRQueries(env, dbi);  
                    end = std::chrono::high_resolution_clock::now();
                    insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                    
                    std::cout <<i+1<< " turns, SequentialQueryCSR took " << insert_duration.count() << " milliseconds" << std::endl;
                }
            } else {
                for (int i = 0; i < 3; ++i) {  // Run the tests 3 times
                    cout << "Starting test round " << i + 1 << endl;

                    start = std::chrono::high_resolution_clock::now();

                    performRandomCSRQueries(env, dbi);  
                    end = std::chrono::high_resolution_clock::now();
                    insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                
                    std::cout <<i+1<< " turns, NonSequentialQueryCSR took " << insert_duration.count() << " milliseconds" << std::endl;
                }
            }
        }
    }

    mdb_dbi_close(env, dbi);
    mdb_env_close(env);

    return 0;    // return 0;

}

