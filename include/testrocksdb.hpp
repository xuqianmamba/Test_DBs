
#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>
#include <fstream>
#include <chrono>
#include <omp.h>
#include <random>
#include <algorithm>
#include <thread>
#include <vector>
#include <bit>
#include <netinet/in.h> // 包含字节序转换函数
#include "rocksdb/statistics.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/comparator.h"

using namespace std;
using namespace rocksdb;
std::mutex mtx;

#define DTYPE uint32_t

//For sorted COO
inline void ParseKey(const rocksdb::Slice& key, DTYPE* part1, DTYPE* part2) {
    *part1 = *reinterpret_cast<const DTYPE*>(key.data());
    *part2 = *reinterpret_cast<const DTYPE*>(key.data() + sizeof(DTYPE));
}


class TwoPartComparator : public rocksdb::Comparator {
public:
    inline int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
      DTYPE a1, a2, b1, b2;
      ParseKey(a, &a1, &a2);
      ParseKey(b, &b1, &b2);
      if (a1 < b1) return -1;
      if (a1 > b1) return +1;
      if (a2 < b2) return -1;
      if (a2 > b2) return +1;
      return 0;
    }
    

    // Ignore the following methods for now:
    const char* Name() const { return "TwoPartComparator"; }
    void FindShortestSeparator(std::string*, const rocksdb::Slice&) const { }
    void FindShortSuccessor(std::string*) const { }
};

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

inline void Store_COO(rocksdb::DB* db, std::vector<DTYPE>& csr_vlist, std::vector<DTYPE>& csr_elist) {
    rocksdb::WriteOptions write_options;
    for (size_t vertex_id = 0; vertex_id < csr_vlist.size() - 1; vertex_id++) {
        DTYPE start_pos = csr_vlist[vertex_id];
        DTYPE end_pos = csr_vlist[vertex_id + 1];
        std::vector<DTYPE> adjacency_list(csr_elist.begin() + start_pos, csr_elist.begin() + end_pos);

        // Insert each edge (src, dst) into RocksDB
        for (DTYPE dst : adjacency_list) {
            std::pair<DTYPE, DTYPE> edge = std::make_pair(vertex_id, dst);

            Status status = db->Put(write_options,
                                    rocksdb::Slice(reinterpret_cast<const char*>(&edge), sizeof(edge)),
                                    rocksdb::Slice(""));
            if (!status.ok()) {
                std::cerr << "Error storing COO data: " << status.ToString() << std::endl;
            }
            assert(status.ok());
        }
    }
}


void Store_CSR(rocksdb::DB* db, std::vector<DTYPE>& csr_vlist, std::vector<DTYPE>& csr_elist) {
    for (DTYPE vertex_id = 0; vertex_id < csr_vlist.size() - 1; vertex_id++) {
        DTYPE start_pos = csr_vlist[vertex_id];
        DTYPE end_pos = csr_vlist[vertex_id + 1];
        std::vector<DTYPE> adjacency_list(csr_elist.begin() + start_pos, csr_elist.begin() + end_pos);

        // Create a value buffer that holds the adjacency list data
        std::vector<char> value_buffer;
        value_buffer.insert(value_buffer.end(), reinterpret_cast<const char*>(adjacency_list.data()), reinterpret_cast<const char*>(adjacency_list.data() + adjacency_list.size()));

        Status status = db->Put(WriteOptions(),
            rocksdb::Slice(reinterpret_cast<const char*>(&vertex_id), sizeof(DTYPE)),
            rocksdb::Slice(value_buffer.data(), value_buffer.size()));

        assert(status.ok());
    }
}


std::vector<DTYPE> Get_Single_Adjacency_List_CSR(rocksdb::DB* db, DTYPE vertex_id) {
    rocksdb::ReadOptions read_options;
    std::string value;
    Status status = db->Get(read_options, rocksdb::Slice(reinterpret_cast<const char*>(&vertex_id), sizeof(vertex_id)), &value);
    if (status.ok()) {
        // Create a vector with the correct size to hold the data
        std::vector<DTYPE> adjacency_list(value.size()  / sizeof(DTYPE));

        // Copy the data from the string to the vector using memcpy, starting after the visited flag
        memcpy(adjacency_list.data(), value.data() , value.size() );

        return adjacency_list;
    } else {
        std::cerr << "Error reading adjacency list for vertex " << vertex_id << ": " << status.ToString() << std::endl;
        return std::vector<DTYPE>();
    }
}

void K_Hop_Neighbors_CSR(rocksdb::DB* db, DTYPE start_vertex, DTYPE k,DTYPE vertexes) {
    std::vector<bool> visited_bitmap(vertexes, false); // Assuming NODE_COUNT is the total number of nodes in your graph
    std::vector<DTYPE> frontier;
    std::vector<DTYPE> next_frontier;
    size_t total_visited_nodes = 0;
    size_t total_visited_edges = 0;

    frontier.push_back(start_vertex);
    visited_bitmap[start_vertex] = true;

    for (DTYPE hop = 0; hop < k; hop++) {
        next_frontier.clear();
        for (size_t i = 0; i < frontier.size(); i++) {
            total_visited_nodes++;
            DTYPE vertex_id = frontier[i];
            std::vector<DTYPE> adjacency_list = Get_Single_Adjacency_List_CSR(db, vertex_id);
            for (DTYPE neighbor : adjacency_list) {
                total_visited_edges++;
                if (!visited_bitmap[neighbor]) {
                    next_frontier.push_back(neighbor);
                    visited_bitmap[neighbor] = true;
                }
            }
        }
        frontier = next_frontier;
    }

    std::cout << "Number of K-hop neighbors of vertex " << start_vertex << " (K = " << k << "): ";
    std::cout << frontier.size() << std::endl;
    std::cout << "total nodes " << total_visited_nodes << std::endl;
    std::cout << "total edges " << total_visited_edges << std::endl;
    std::cout << std::endl;
}

//For parallel query
void process_CSR(int start, int end, const std::vector<DTYPE>& frontier, std::vector<DTYPE>& next_frontier, std::vector<bool>& visited_bitmap, rocksdb::DB* db) {
    for (int i = start; i < end; i++) {
        DTYPE vertex_id = frontier[i];
        std::vector<DTYPE> adjacency_list = Get_Single_Adjacency_List_CSR(db, vertex_id);
        for (DTYPE neighbor : adjacency_list) {
            if (!visited_bitmap[neighbor]) {
                mtx.lock();
                next_frontier.push_back(neighbor);
                mtx.unlock();
                visited_bitmap[neighbor] = true;
            }
        }
    }
}

void Para_K_Hop_Neighbors_CSR(rocksdb::DB* db, DTYPE start_vertex, DTYPE k,DTYPE vertexes) {
    std::vector<bool> visited_bitmap(vertexes, false); 
    std::vector<DTYPE> frontier;
    std::vector<DTYPE> next_frontier;
    size_t total_visited_nodes = 0;
    size_t total_visited_edges = 0;

    frontier.push_back(start_vertex);
    visited_bitmap[start_vertex] = true;

    int num_threads = 10; // or std::thread::hardware_concurrency()

    for (DTYPE hop = 0; hop < k; hop++) {
        next_frontier.clear();
        std::vector<std::thread> threads;
        int chunk_size = (frontier.size() + num_threads - 1) / num_threads;
        for (int i = 0; i < frontier.size(); i += chunk_size) {
            int end = std::min(i + chunk_size, (int)frontier.size());
            threads.push_back(std::thread(process_CSR, i, end, std::ref(frontier), std::ref(next_frontier), std::ref(visited_bitmap), db));
        }
        for(auto& th : threads) th.join();
        std::sort( next_frontier.begin(), next_frontier.end() );
        next_frontier.erase( std::unique( next_frontier.begin(), next_frontier.end() ), next_frontier.end() );
        frontier = next_frontier;
    }
    // Rest of the code...
}

std::vector<std::pair<DTYPE, DTYPE>> Range_Query_Neighbors_COO(rocksdb::DB* db, size_t vertex_id) {
    rocksdb::ReadOptions read_options;
    rocksdb::Slice start_key(reinterpret_cast<const char*>(&vertex_id), sizeof(vertex_id));
    // To get the next vertex's adjacency list, we need to increase the vertex_id by 1
    size_t next_vertex_id = vertex_id + 1;
    rocksdb::Slice end_key(reinterpret_cast<const char*>(&next_vertex_id), sizeof(next_vertex_id));
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options));
    it->Seek(start_key);
    std::vector<std::pair<DTYPE, DTYPE>> neighbors;
    while (it->Valid() && it->key().compare(end_key) < 0) {
        std::pair<DTYPE, DTYPE> key_value = *reinterpret_cast<const std::pair<DTYPE, DTYPE>*>(it->key().data());
        // cout<<key_value.first<<" "<<key_value.second<<endl;
        neighbors.push_back(key_value);

        it->Next();
    }
    return neighbors;
}

void K_Hop_Neighbors_COO(rocksdb::DB* db, size_t start_vertex, size_t k,DTYPE vertexes) {
    std::vector<DTYPE> frontier;
    std::vector<DTYPE> next_frontier;
    size_t total_visited_nodes = 0;
    size_t total_visited_edges = 0;
    std::vector<bool> visited_bitmap(vertexes, false);
    
    frontier.push_back(start_vertex);
    visited_bitmap[start_vertex] = true;

    for (size_t hop = 0; hop < k; hop++) {
        next_frontier.clear();
        for (size_t vertex_id : frontier) {
            total_visited_nodes++;
            std::vector<std::pair<DTYPE, DTYPE>> neighbors = Range_Query_Neighbors_COO(db, vertex_id);
            for (const auto& neighbor : neighbors) {
                total_visited_edges++;
                DTYPE dst = neighbor.second;
                if (visited_bitmap[dst] == 0) {
                    visited_bitmap[dst] = 1; // Mark the vertex as visited
                    next_frontier.push_back(dst);
                }
            }
        }
        frontier = next_frontier;
    }

    std::cout << "Number of K-hop neighbors of vertex " << start_vertex << " (K = " << k << "): ";
    std::cout << frontier.size() << std::endl;
    std::cout << "total nodes " << total_visited_nodes << std::endl;
    std::cout << "total edges " << total_visited_edges << std::endl;
    std::cout << std::endl;
}


void Para_K_Hop_Neighbors_COO(rocksdb::DB* db, size_t start_vertex, size_t k,DTYPE vertexes) {
    std::vector<DTYPE> frontier;
    std::vector<DTYPE> next_frontier;
    size_t total_visited_nodes = 0;
    size_t total_visited_edges = 0;
    std::vector<bool> visited_bitmap(vertexes, false);

    frontier.push_back(start_vertex);
    visited_bitmap[start_vertex] = true;

    for (size_t hop = 0; hop < k; hop++) {
        next_frontier.clear();
        #pragma omp parallel for
        for (int i = 0; i < frontier.size(); i++) {
            total_visited_nodes++;
            size_t vertex_id = frontier[i];
            std::vector<std::pair<DTYPE, DTYPE>> neighbors = Range_Query_Neighbors_COO(db, vertex_id);
            for (const auto& neighbor : neighbors) {
                total_visited_edges++;
                DTYPE dst = neighbor.second;
                if (visited_bitmap[dst] == 0) {
                    visited_bitmap[dst] = 1; 
                    #pragma omp critical
                    next_frontier.push_back(dst);
                }
            }
        }
        std::sort( next_frontier.begin(), next_frontier.end() );
        next_frontier.erase( std::unique( next_frontier.begin(), next_frontier.end() ), next_frontier.end() );
        frontier = next_frontier;
    }
    std::cout << "Number of K-hop neighbors of vertex " << start_vertex << " (K = " << k << "): ";
    std::cout << frontier.size() << std::endl;
    std::cout << "total nodes " << total_visited_nodes << std::endl;
    std::cout << "total edges " << total_visited_edges << std::endl;
    std::cout << std::endl;
}


void sequentialQueryCOO(DB* db, int start, int end) {
    for (int i = start; i < end; ++i) {
        Range_Query_Neighbors_COO(db, i);
    }
}

void noequentialQueryCSR(DB* db, int start, int end, int thread_idx, std::vector<int>& counts, std::vector<int>& sizes) {
    int count = 0;
    int total_size = 0;

    std::vector<int> indices(end - start);
    for (int i = 0; i < end - start; ++i) {
        indices[i] = start + i;
    }

    // random access
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(indices.begin(), indices.end(), g);

    for (int i : indices) {
        auto result = Get_Single_Adjacency_List_CSR(db,i);
        count++;
        total_size += result.size();
    }

    counts[thread_idx] = count;
    sizes[thread_idx] = total_size;
}

void sequentialQueryCSRWithIterator(rocksdb::DB* db, int start, int end, int thread_idx, std::vector<int>& counts, std::vector<int>& sizes) {
    int count = 0;
    int total_size = 0;
    std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(rocksdb::ReadOptions()));
    for (DTYPE i = start; i < end; ++i) {
        rocksdb::Slice key(reinterpret_cast<const char*>(&i), sizeof(DTYPE));
        iter->Seek(key);
        if (iter->Valid() && iter->key().compare(key) == 0) {
            size_t list_size = iter->value().size() / sizeof(DTYPE);
            count++;
            total_size += list_size;
        }
    }
    counts[thread_idx] = count;
    sizes[thread_idx] = total_size;
    // cout<<"count "<< count;
    // cout<<"total_size "<< total_size;
}

void performCOOQueries(DB* coo_db) {
    int n_threads = 10;
    int data_per_thread = 1000000; 
    int offset = 500000; 

    std::vector<std::thread> threads;
    for (int i = 0; i < n_threads; ++i) {
        int start = i * offset;
        threads.emplace_back(sequentialQueryCOO, coo_db, start, start + data_per_thread);
    }
    for (auto& t : threads) t.join();
}

void performCSRQueries(DB* csr_db) {
    int n_threads = 10;
    int data_per_thread = 4000000; 
    int offset = 100000; 

    std::vector<int> counts(n_threads, 0);
    std::vector<int> sizes(n_threads, 0);

    std::vector<std::thread> threads;
    for (int i = 0; i < n_threads; ++i) {
        int start = i * offset;
        threads.emplace_back(noequentialQueryCSR, csr_db, start, start + data_per_thread, i, std::ref(counts), std::ref(sizes));
    }

    for (auto& t : threads) t.join();

    //print
    for (int i = 0; i < n_threads; ++i) {
        std::cout << "Thread " << i << ": Calls = " << counts[i] << ", Total Size = " << sizes[i] << std::endl;
    }
}

void performCSRQueriesIter(rocksdb::DB* csr_db) {
    int n_threads = 10;
    int data_per_thread = 4000000; 
    int offset = 4100000; 

    std::vector<int> counts(n_threads, 0);
    std::vector<int> sizes(n_threads, 0);

    std::vector<std::thread> threads;
    for (int i = 0; i < n_threads; ++i) {
        int start = i * offset;
        threads.emplace_back(sequentialQueryCSRWithIterator, csr_db, start, start + data_per_thread, i, std::ref(counts), std::ref(sizes));
    }

    for (auto& t : threads) t.join();
    int total=0;
    // print
    for (int i = 0; i < n_threads; ++i) {
        std::cout << "Thread " << i << ": Calls = " << counts[i] << ", Total Size = " << sizes[i] << std::endl;
        total+=sizes[i];
    }
    cout<<endl<<"Total size is"<<total<<endl;
}

//Set start Iter postision
const std::vector<uint64_t> start_keys = {
    0,
    422767105,
    845535233,
    1268304640,
    1691079169,
    2113853952,
    2536622081,
    2959390209,
    3382159361,
    3804934144
};

std::string int_to_network_byte_order(int num) {
    uint32_t net_order = htonl(num);
    return std::string(reinterpret_cast<char*>(&net_order), sizeof(net_order));
}



#include <iostream>
