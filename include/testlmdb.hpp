#include <lmdb.h>
#include <vector>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <omp.h>
#include <random>
#include <algorithm>
#include <thread>
#include <cstring>
#include <arpa/inet.h>
using namespace std;

#define DTYPE uint32_t

inline void Load_Vertex_File(const std::string path, std::vector<DTYPE>& sequence) {
    DTYPE max_symbol = 0;
    DTYPE len;
    DTYPE tmp;
    std::ifstream infile(path);
    if (!infile.good()) {
        fprintf(stderr, "file not exist:%s\n", path.c_str());
        exit(0);
    }
    while (infile >> tmp) {
        sequence.emplace_back(tmp);
    }
    return;
}

inline void Store_CSR(MDB_env* env, MDB_dbi dbi, std::vector<DTYPE>& csr_vlist, std::vector<DTYPE>& csr_elist) {
    MDB_txn* txn;
    cout<<"len elist"<<csr_elist.size()<<endl;
    cout<<"len vlist"<<csr_vlist.size()<<endl;

    int result = mdb_txn_begin(env, nullptr, 0, &txn);
    if (result != MDB_SUCCESS) {
        std::cerr << "Error beginning transaction: " << mdb_strerror(result) << std::endl;
        return;
    }
    
    result = mdb_dbi_open(txn, NULL, 0, &dbi);
    if(result){
        printf("mdb_dbi_open error,detail:%s\n", mdb_strerror(result));
        return;
    }

    for (uint32_t vertex_id = 0; vertex_id < csr_vlist.size() - 1; vertex_id++) {
        DTYPE* writable_vertex_id = &vertex_id;
        MDB_val mdbKey = {sizeof(*writable_vertex_id), writable_vertex_id};
        
        DTYPE start_pos = csr_vlist[vertex_id];
        DTYPE end_pos = csr_vlist[vertex_id + 1];
        
        std::vector<DTYPE> adjacency_list(csr_elist.begin() + start_pos, csr_elist.begin() + end_pos);
        DTYPE* writable_adjacency_list = new DTYPE[adjacency_list.size()];
        std::copy(adjacency_list.begin(), adjacency_list.end(), writable_adjacency_list);

        MDB_val mdbData = {adjacency_list.size() * sizeof(DTYPE), writable_adjacency_list};
        

        result = mdb_put(txn, dbi, &mdbKey, &mdbData, 0);
        if (result != MDB_SUCCESS) {
            std::cerr << "Error putting data: " << mdb_strerror(result) << std::endl;
            delete[] writable_adjacency_list; // Release allocated memory
            mdb_txn_abort(txn);
            return;
        }

        delete[] writable_adjacency_list; // Release allocated memory
    }

    result = mdb_txn_commit(txn);
    if (result != MDB_SUCCESS) {
        std::cerr << "Error committing transaction: " << mdb_strerror(result) << std::endl;
        return;
    }
}

// inline std::vector<DTYPE> Get_Single_Adjacency_List_CSR_Cursor(MDB_env* env, MDB_dbi& dbi, DTYPE vertex_id) {
//     MDB_txn* txn;
//     mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);

//     MDB_val mdbKey, mdbData;

//     DTYPE* keyData = new DTYPE(vertex_id);
//     mdbKey.mv_size = sizeof(DTYPE);
//     mdbKey.mv_data = keyData;

//     int result = mdb_get(txn, dbi, &mdbKey, &mdbData);
//     if (result == MDB_SUCCESS) {
//         DTYPE* adjacency_list_data = reinterpret_cast<DTYPE*>(mdbData.mv_data);
//         size_t list_size = mdbData.mv_size / sizeof(DTYPE);

//         std::vector<DTYPE> adjacency_list(adjacency_list_data, adjacency_list_data + list_size);
//         mdb_txn_abort(txn);
//         delete keyData; 
//         return adjacency_list;
//     } else {
//         mdb_txn_abort(txn);
//         std::cerr << "Error reading adjacency list for vertex " << vertex_id << ": " << mdb_strerror(result) << std::endl;
//         delete keyData; 
//         return std::vector<DTYPE>();
//     }
// }

inline std::vector<DTYPE> Get_Single_Adjacency_List_CSR(MDB_env* env, MDB_dbi& dbi, DTYPE vertex_id) {
    MDB_txn* txn;
    mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);

    int result = mdb_dbi_open(txn, NULL, 0, &dbi);
    if(result){
        printf("mdb_dbi_open error,detail:%s\n", mdb_strerror(result));
        return std::vector<DTYPE>();
    }

    MDB_val mdbKey, mdbData;

    DTYPE* keyData = new DTYPE(vertex_id);
    mdbKey.mv_size = sizeof(DTYPE);
    mdbKey.mv_data = keyData;

    result = mdb_get(txn, dbi, &mdbKey, &mdbData);
    if (result == MDB_SUCCESS) {
        DTYPE* adjacency_list_data = reinterpret_cast<DTYPE*>(mdbData.mv_data);
        size_t list_size = mdbData.mv_size / sizeof(DTYPE);

        std::vector<DTYPE> adjacency_list(adjacency_list_data, adjacency_list_data + list_size);
        mdb_txn_abort(txn);
        delete keyData; 
        return adjacency_list;
    } else {
        mdb_txn_abort(txn);
        std::cerr << "Error reading adjacency list for vertex " << vertex_id << ": " << mdb_strerror(result) << std::endl;
        delete keyData; 
        return std::vector<DTYPE>();
    }
}

inline void NoSequentialQueryCSR(MDB_env* env, MDB_dbi dbi, int start, int end, int thread_idx, std::vector<int>& counts, std::vector<int>& sizes) {
    int count = 0;
    int total_size = 0;

    std::vector<int> indices(end - start);
    for (int i = 0; i < end - start; ++i) {
        indices[i] = start + i;
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(indices.begin(), indices.end(), g);

    for (int i : indices) {
        auto result = Get_Single_Adjacency_List_CSR(env, dbi, i);
        count++;
        total_size += result.size();
    }

    counts[thread_idx] = count;
    sizes[thread_idx] = total_size;
}

inline void SequentialQueryCSR(MDB_env* env, MDB_dbi dbi, int start, int end, int thread_idx, std::vector<int>& counts, std::vector<int>& sizes) {
    int count = 0;
    int total_size = 0;

    for (int i = start; i < end; ++i) {
        auto result = Get_Single_Adjacency_List_CSR(env, dbi, i);
        count++;
        total_size += result.size();
    }

    counts[thread_idx] = count;
    sizes[thread_idx] = total_size;
}

// This function uses the LMDB's cursor feature to return an adjacency list of a vertex.
inline std::vector<DTYPE> Get_Adjacency_List_By_Cursor(MDB_cursor* cursor) {
    MDB_val mdbKey, mdbData;
    int result = mdb_cursor_get(cursor, &mdbKey, &mdbData, MDB_NEXT);
    
    if (result == MDB_SUCCESS) {
        DTYPE* adjacency_list_data = reinterpret_cast<DTYPE*>(mdbData.mv_data);
        size_t list_size = mdbData.mv_size / sizeof(DTYPE);
        return std::vector<DTYPE>(adjacency_list_data, adjacency_list_data + list_size);
    } else {
        return std::vector<DTYPE>();
    }
}



inline void SequentialQueryCSRCursor(MDB_env* env, MDB_dbi dbi, int start, int end, int thread_idx, std::vector<int>& counts, std::vector<int>& sizes) {
    MDB_txn* txn;
    mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);

    MDB_cursor* cursor;
    mdb_cursor_open(txn, dbi, &cursor);

    DTYPE keyData = static_cast<DTYPE>(start); 
    // cout<<"keyData  "<<keyData<<endl;
    MDB_val mdbKey = {sizeof(DTYPE), &keyData};
    mdb_cursor_get(cursor, &mdbKey,nullptr, MDB_SET_RANGE);

    int count = 0;
    int total_size = 0;

    for (int i = start; i < end; ++i) {
        auto result = Get_Adjacency_List_By_Cursor(cursor);
        count++;
        total_size += result.size();
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);

    counts[thread_idx] = count;
    sizes[thread_idx] = total_size;
}

inline void performCSRQueries(MDB_env* env, MDB_dbi dbi) {
    int n_threads = 10;
    int data_per_thread = 4000000;
    int offset = 4100000;

    std::vector<int> counts(n_threads, 0);
    std::vector<int> sizes(n_threads, 0);

    std::vector<std::thread> threads;
    for (int i = 0; i < n_threads; ++i) {
        int start = i * offset;
        threads.emplace_back(SequentialQueryCSRCursor, env, dbi, start, start + data_per_thread, i, std::ref(counts), std::ref(sizes));
    }

    for (auto& t : threads) t.join();

    for (int i = 0; i < n_threads; ++i) {
        std::cout << "Thread " << i << ": Calls = " << counts[i] << ", Total Size = " << sizes[i] << std::endl;
    }
}


// inline void K_Hop_Neighbors_CSR(MDB_env* env, MDB_dbi dbi, DTYPE start_vertex, DTYPE k, DTYPE vertexes) {
//     std::vector<bool> visited_bitmap(vertexes, false);
//     std::vector<DTYPE> frontier;
//     std::vector<DTYPE> next_frontier;
//     size_t total_visited_nodes = 0;
//     size_t total_visited_edges = 0;

//     frontier.push_back(start_vertex);
//     visited_bitmap[start_vertex] = true;

//     for (DTYPE hop = 0; hop < k; hop++) {
//         next_frontier.clear();
//         for (size_t i = 0; i < frontier.size(); i++) {
//             total_visited_nodes++;
//             DTYPE vertex_id = frontier[i];
//             std::vector<DTYPE> adjacency_list = Get_Single_Adjacency_List_CSR(env, dbi, vertex_id);
//             for (DTYPE neighbor : adjacency_list) {
//                 total_visited_edges++;
//                 if (!visited_bitmap[neighbor]) {
//                     next_frontier.push_back(neighbor);
//                     visited_bitmap[neighbor] = true;
//                 }
//             }
//         }
//         frontier = next_frontier;
//     }

//     std::cout << "Number of K-hop neighbors of vertex " << start_vertex << " (K = " << k << "): ";
//     std::cout << frontier.size() << std::endl;
//     std::cout << "total nodes " << total_visited_nodes << std::endl;
//     std::cout << "total edges " << total_visited_edges << std::endl;
//     std::cout << std::endl;
// }


inline void PrintAllKeysInLMDB(MDB_env* env, MDB_dbi dbi) {
    MDB_txn* txn;
    MDB_cursor* cursor;
    MDB_val mdbKey, mdbData;

    int result = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
    if (result != MDB_SUCCESS) {
        std::cerr << "Error starting transaction: " << mdb_strerror(result) << std::endl;
        return;
    }

    result = mdb_cursor_open(txn, dbi, &cursor);
    if (result != MDB_SUCCESS) {
        std::cerr << "Error opening cursor: " << mdb_strerror(result) << std::endl;
        mdb_txn_abort(txn);
        return;
    }

    while (mdb_cursor_get(cursor, &mdbKey, &mdbData, MDB_NEXT) == MDB_SUCCESS) {
        DTYPE* key = reinterpret_cast<DTYPE*>(mdbKey.mv_data);
        std::cout << *key << std::endl;
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
}


inline std::string int_to_network_byte_order(int i) {
    uint32_t net_value = htonl(static_cast<uint32_t>(i));
    return std::string(reinterpret_cast<char*>(&net_value), sizeof(uint32_t));
}

inline std::vector<DTYPE> Get_Adjacency_List_By_Random_Access(MDB_cursor* cursor, DTYPE randomKey) {
    MDB_val mdbKey = {sizeof(DTYPE), &randomKey}, mdbData;

    int result = mdb_cursor_get(cursor, &mdbKey, &mdbData, MDB_SET);

    if (result == MDB_SUCCESS) {
        DTYPE* adjacency_list_data = reinterpret_cast<DTYPE*>(mdbData.mv_data);
        size_t list_size = mdbData.mv_size / sizeof(DTYPE);
        return std::vector<DTYPE>(adjacency_list_data, adjacency_list_data + list_size);
    } else {
        return std::vector<DTYPE>();
    }
}

inline void RandomAccessQueryCSRCursor(MDB_env* env, MDB_dbi dbi, const std::vector<DTYPE>& randomKeys, int thread_idx, std::vector<int>& counts, std::vector<int>& sizes) {
    MDB_txn* txn;
    mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);

    MDB_cursor* cursor;
    mdb_cursor_open(txn, dbi, &cursor);

    int count = 0;
    int total_size = 0;

    for (DTYPE key : randomKeys) {
        auto result = Get_Adjacency_List_By_Random_Access(cursor, key);
        count++;
        total_size += result.size();
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);

    counts[thread_idx] = count;
    sizes[thread_idx] = total_size;
}

inline void performRandomCSRQueries(MDB_env* env, MDB_dbi dbi) {
    int n_threads = 10;
    int keys_per_thread = 4000000;  

    std::vector<int> counts(n_threads, 0);
    std::vector<int> sizes(n_threads, 0);

    std::vector<std::thread> threads;
    for (int i = 0; i < n_threads; ++i) {
        std::vector<DTYPE> randomKeys;
        
        for (int j = 0; j < keys_per_thread; ++j) {
            randomKeys.push_back(static_cast<DTYPE>(rand() % keys_per_thread)); 
        }

        threads.emplace_back(RandomAccessQueryCSRCursor, env, dbi, randomKeys, i, std::ref(counts), std::ref(sizes));
    }

    for (auto& t : threads) t.join();

    for (int i = 0; i < n_threads; ++i) {
        std::cout << "Thread " << i << ": Calls = " << counts[i] << ", Total Size = " << sizes[i] << std::endl;
    }
}
