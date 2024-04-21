#ifndef BATCH_PARALLEL_MAP_HPP
#define BATCH_PARALLEL_MAP_HPP

#include <vector>
#include <fstream>
#include <algorithm>
#include <iostream>
#include <omp.h>
#include "PAM/include/pam/pam.h"
#include <iostream>
#include <unordered_map>
#include <vector>
#include <thread>
#include <omp.h>
#include <chrono>
#include <mutex>
#include "threadpool.hpp"
#include "concurrentqueue/concurrentqueue.h" //https://github.com/cameron314/concurrentqueue
#include <cxxabi.h>
#include "tbb/concurrent_hash_map.h"
#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"


template<typename Key, typename Value>
struct entry {
using key_t = Key;
using val_t = Value;
static bool comp(key_t a, key_t b) { 
    return a < b;}
};

template<typename Key, typename Value>
class BatchParallelConcurrentHashMap {
private:

    pam_map<entry<Key, Value>> map_;
    moodycamel::ConcurrentQueue<std::pair<Key, Value>> insert_queue;
    moodycamel::ConcurrentQueue<Key> delete_queue;
    moodycamel::ConcurrentQueue<Key> find_queue;
    std::mutex insertMutex, deleteMutex;
    int approx_size_bound = 10000;
    int dequeue_buffer_size = approx_size_bound * 2;

public:
    bool supports_batch_insert = true;
    bool supports_batch_delete = true;
    bool supports_multi_get = false;

    BatchParallelConcurrentHashMap(int batch_size = 10000) {
        approx_size_bound = batch_size;
        dequeue_buffer_size = batch_size * 2;
    }

    void batch_insert(const Key& key, const Value& value) {
        insert_queue.enqueue(std::make_pair(key, value));
        if(insert_queue.size_approx() > approx_size_bound) {
            std::pair<Key, Value>* tmp = new std::pair<Key, Value>[dequeue_buffer_size];
            size_t count = insert_queue.try_dequeue_bulk(tmp, dequeue_buffer_size);
            std::vector<std::pair<Key, Value>> batch(tmp, tmp + count);
            std::lock_guard<std::mutex> guard(insertMutex);
            map_ = map_.multi_insert(map_, batch);
        }
    }

    void batch_delete(const Key& key) {
        delete_queue.enqueue(key);
        if(delete_queue.size_approx() > approx_size_bound) {
            Key tmp[dequeue_buffer_size];
            size_t count = delete_queue.try_dequeue_bulk(tmp, dequeue_buffer_size);
            std::vector<Key> batch(tmp, tmp + count);
            std::lock_guard<std::mutex> guard(deleteMutex);
            map_ = map_.multi_delete(map_, batch);
        }
    }

    void batch_find(const Key& key) {
        // find_queue.enqueue(key);
        // if(find_queue.size_approx() > approx_size_bound) {
        //     Key tmp[dequeue_buffer_size];
        //     size_t count = find_queue.try_dequeue_bulk(tmp, dequeue_buffer_size);
        //     std::vector<Key> batch(tmp, tmp + count);
        //     map_ = map_.multi_find(map_, batch);
        // }
    }

    void finalize_batch_insert() {
        std::pair<Key, Value>* tmp = new std::pair<Key, Value>[dequeue_buffer_size];
        size_t count = insert_queue.try_dequeue_bulk(tmp, dequeue_buffer_size);
        std::vector<std::pair<Key, Value>> batch(tmp, tmp + count);
        std::lock_guard<std::mutex> guard(insertMutex);
        map_ = map_.multi_insert(map_, batch);
    }
    
    void finalize_batch_delete() {
        Key tmp[dequeue_buffer_size];
        size_t count = delete_queue.try_dequeue_bulk(tmp, dequeue_buffer_size);
        std::vector<Key> batch(tmp, tmp + count);
        std::lock_guard<std::mutex> guard(deleteMutex);
        map_ = map_.multi_delete(map_, batch);
    }

    void insert(const Key& key, const Value& value) {
        map_.insert(std::make_pair(key, value));
    }

    void remove(const Key& key) {
        map_ = map_.remove(map_, key);
    }

    Value get(const Key& key)/* const*/ {
        //finalize_batch_insert();
        //finalize_batch_delete();
        auto it = map_.find(key);
        if(it.has_value()) {
            return it.value();
        }
        return Value{};
    }

    Value* multi_get(const std::vector<Key>& keys) const {
        // Sort the keys
        // std::sort(keys.begin(), keys.end());
        // auto keys2 = parlay::make_slice(keys);
        // auto keys3 = parlay::internal::sample_sort(parlay::make_slice(keys), entry<Key, Value>::comp);
        // auto it = map_.multi_find(map_, keys3);
        return nullptr;
    }

};

template<typename Key, typename Value>
class PAMConcurrentHashMap {
    private:
        pam_map<entry<Key, Value>> map_;

    public:
        bool supports_batch_insert = false;
        bool supports_batch_delete = false;
        bool supports_multi_get = false;

        void insert(const Key& key, const Value& value) {
            map_.insert(std::make_pair(key, value));
        }


        void finalize_batch_insert() {
            // Do nothing
        }

        void batch_insert(const Key& key, const Value& value) {
            map_.insert(std::make_pair(key, value));
        }

        void batch_delete(const Key& key) {
            map_ = map_.remove(map_, key);
        }

        void remove(const Key& key) {
            map_ = map_.remove(map_, key);
        }

        void finalize_batch_delete() {
            // Do nothing
        }

        Value get(const Key& key) const {
            auto it = map_.find(key);
            if(it.has_value()) {
                return it.value();
            }
            return Value{};
        }
    
        Value* multi_get(std::vector<Key>& keys) const {
            // not implemented
            return nullptr;
        }
};

template<typename Key, typename Value>
class TBBConcurrentHashMap {
private:
    tbb::concurrent_hash_map<Key, Value> map_;

public:
    bool supports_batch_insert = false;
    bool supports_batch_delete = false;
    bool supports_multi_get = false;

    TBBConcurrentHashMap() {}

    void insert(const Key& key, const Value& value) {
        typename tbb::concurrent_hash_map<Key, Value>::accessor accessor;
        map_.insert(accessor, key);
        accessor->second = value;
        accessor.release();
    }

    void _delete(const Key& key) {
        map_.erase(key);
    }

    void batch_insert(const Key& key, const Value& value) {
        insert(key, value);
    }

    void remove(const Key& key) {
        _delete(key);
    }

    Value get(const Key& key) {
        typename tbb::concurrent_hash_map<Key, Value>::const_accessor accessor;
        if (map_.find(accessor, key)) {
            return accessor->second;
        }
        return Value{}; // Return default value if not found
    }
};


// Write a test class to test latency of each data structure
// Test insert and get operations
// Test with different number of threads
// Test with different batch sizes
// Test with different number of elements
// Test with different number of buckets
// Test with different hash functions
// Test with different key-value types


template<typename DataStructure>
class BatchParallelMapTest {
public:
    void test_insert_latency(int n = 1000000, int num_threads = 1) {
        int k = 1000000;
        DataStructure data_structure;
        // set omp number of threads
        auto start = std::chrono::high_resolution_clock::now();
        auto stop = std::chrono::high_resolution_clock::now();
        if (!data_structure.supports_batch_insert) {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (uint64_t i = 0; i < n; ++i)
                data_structure.insert(i%k, "Value " + std::to_string(i));
            stop = std::chrono::high_resolution_clock::now();
            
        } else {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (uint64_t i = 0; i < n; ++i) 
                data_structure.batch_insert(i%k, "Value " + std::to_string(i));
            data_structure.finalize_batch_insert();
            stop = std::chrono::high_resolution_clock::now();
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::string name = typeid(DataStructure).name();
        int status;
        char* demangled_name = abi::__cxa_demangle(name.c_str(), 0, 0, &status);
        demangled_name = strtok(demangled_name, "<");
        std::cout << "Time taken by " << demangled_name << " for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
        std::cout << "Value for key 10: " << data_structure.get(10) << std::endl;
        std::cout << "Value for key " << (n-1)%k << ": " << data_structure.get((n-1)%k) << std::endl;

        // check for correctness
        // std::vector<int> keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        // std::string* values = data_structure.multi_get(keys);
        // for (int i = 0; i < keys.size(); ++i) {
        //     std::cout << "Value for key " << keys[i] << ": " << values[i] << std::endl;
        // }

    }

    void test_ycsb_file(const std::string& loadFile, const std::string& txnFile, int num_threads) {
        DataStructure map;
        std::ifstream fileLoad(loadFile);
        std::ifstream fileTxn(txnFile);

        auto loadStart = std::chrono::high_resolution_clock::now();
        auto loadStop = std::chrono::high_resolution_clock::now();

        size_t sample_size = 10000000;

        if (!fileLoad.is_open() || !fileTxn.is_open()) {
            std::cerr << "Error opening files: " << loadFile << " or " << txnFile << std::endl;
            return;
        }

        // Preprocess commands into a structured vector
        std::vector<std::pair<std::string, uint64_t>> operations;
        std::string line;
        size_t total_lines = 0;
        cout<<"starting Load"<<endl;
        loadStart = std::chrono::high_resolution_clock::now();
        while (std::getline(fileLoad, line)) {
            std::istringstream iss(line);
            std::string cmd;
            uint64_t key;
            iss >> cmd >> key;
            if (cmd == "INSERT") {
                map.insert(key, "Value " + std::to_string(key));
            }
        }
        loadStop = std::chrono::high_resolution_clock::now();
        auto loadDuration = std::chrono::duration_cast<std::chrono::microseconds>(loadStop - loadStart);

        fileLoad.close();
        cout<<"Load File Processing Complete"<<endl;
        cout<<"Load took: "<<loadDuration.count()<<" microseconds"<<endl;

        while (std::getline(fileTxn, line)) {
            ++total_lines;
        }

        fileTxn.clear();
        fileTxn.seekg(0, std::ios::beg);

        // Uniform sampling
        cout<<"Total Lines: "<<total_lines<<endl;
        double sampling_rate = static_cast<double>(sample_size) / total_lines;
        std::mt19937 rng;  // Random number generator
        std::uniform_real_distribution<double> dist(0.0, 1.0);

        while (std::getline(fileTxn, line)) {
            //cout<<dist(rng)<<"''"<<sampling_rate<<endl;
            if (dist(rng) < sampling_rate) {
                std::istringstream iss(line);
                std::string cmd;
                uint64_t key;
                iss >> cmd >> key;
                operations.push_back({cmd, key});
                if (operations.size() >= sample_size) break;
            }
        }
        fileTxn.close();
        cout<<"Txn File processing complete"<<endl;

        //omp_set_num_threads(num_threads);
        cout<<"Threads: "<<num_threads<<endl;
        cout<<"operations: "<<operations.size()<<endl;
        auto start = std::chrono::high_resolution_clock::now();
        auto stop = std::chrono::high_resolution_clock::now();

        if(map.supports_batch_insert) {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (int i = 0; i < operations.size(); ++i){
                auto& op = operations[i];
                if(op.first == "INSERT") {
                    map.batch_insert(op.second, "Value " + std::to_string(op.second));
                } else if(op.first == "READ") {
                    map.get(op.second);
                }
            }
            cout<<"Completed Loop"<<endl;
            //map.finalize_batch_insert();
            stop = std::chrono::high_resolution_clock::now();
        } else {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (int i = 0; i < operations.size(); ++i){
                auto& op = operations[i];
                if(op.first == "INSERT") {
                    map.insert(op.second, "Value " + std::to_string(op.second));
                    //cout<<"Insert"<<endl;
                } else if(op.first == "READ") {
                    map.get(op.second);
                    //cout<<"READ"<<endl;
                }
            }
            stop = std::chrono::high_resolution_clock::now();
        }
        

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

        std::string name = typeid(DataStructure).name();
        int status;
        char* demangled_name = abi::__cxa_demangle(name.c_str(), 0, 0, &status);
        demangled_name = strtok(demangled_name, "<");
        std::cout << "Time taken by " << demangled_name << " to process file " << txnFile 
                  << " with " << num_threads << " threads: " << duration.count() << " microseconds" << std::endl;
    }

    void test_batch_insert_latency(int n = 1000000, int batch_size = 1000, int num_threads = 1) {
        DataStructure data_structure(batch_size);
        // set omp number of threads
        auto start = std::chrono::high_resolution_clock::now();
        auto stop = std::chrono::high_resolution_clock::now();

        // Do only batch insert
        if (data_structure.supports_batch_insert) {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (int i = 0; i < n; ++i) 
                data_structure.batch_insert(i, "Value " + std::to_string(i));
            stop = std::chrono::high_resolution_clock::now();
            data_structure.finalize_batch_insert();
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::string name = typeid(DataStructure).name();
        int status;
        char* demangled_name = abi::__cxa_demangle(name.c_str(), 0, 0, &status);
        demangled_name = strtok(demangled_name, "<");
        // Print the time taken by the data structure, num of inserts and batch size
        std::cout << "Time taken by " << demangled_name << " for " << n << " inserts with batch size " << batch_size << ": " << duration.count() << " microseconds" << std::endl;
        std::cout << "Value for key 10: " << data_structure.get(10) << std::endl;
        std::cout << "Value for key " << n-1 << ": " << data_structure.get(n-1) << std::endl;
        return;
    }

    void test_multi_find(int n = 10000, int batch_size = 1000, int num_threads = 1) {
        int k = 1000000;
        DataStructure data_structure;
        // set omp number of threads
        auto start = std::chrono::high_resolution_clock::now();
        auto stop = std::chrono::high_resolution_clock::now();
        if (!data_structure.supports_batch_insert) {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (uint64_t i = 0; i < n; ++i)
                data_structure.insert(i%k, "Value " + std::to_string(i));
            stop = std::chrono::high_resolution_clock::now();
            
        } else {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (uint64_t i = 0; i < n; ++i) 
                data_structure.batch_insert(i%k, "Value " + std::to_string(i));
            data_structure.finalize_batch_insert();
            stop = std::chrono::high_resolution_clock::now();
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::string name = typeid(DataStructure).name();
        int status;
        char* demangled_name = abi::__cxa_demangle(name.c_str(), 0, 0, &status);
        demangled_name = strtok(demangled_name, "<");
        std::cout << "Time taken by " << demangled_name << " for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
        std::cout << "Value for key 10: " << data_structure.get(10) << std::endl;
        std::cout << "Value for key " << (n-1)%k << ": " << data_structure.get((n-1)%k) << std::endl;

        // auto it = data_structure.multi_get({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        std::vector<int> keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        auto it = data_structure.multi_get(keys);
        for (int i = 0; i < 10; ++i) {
            std::cout << "Value for key " << i << ": " << it[i] << std::endl;
        }
    }

    void test_find_latency(int n = 1000000, int batch_size = 1000, int num_threads = 1) {
        int k = 100000;
        DataStructure data_structure;
        // set omp number of threads
        auto start = std::chrono::high_resolution_clock::now();
        auto stop = std::chrono::high_resolution_clock::now();
        if (!data_structure.supports_batch_insert) {
            #pragma omp parallel for num_threads(num_threads)
            for (uint64_t i = 0; i < n; ++i)
                data_structure.insert(i%k, "Value " + std::to_string(i));
            
        } else {
            start = std::chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < n; ++i) 
                data_structure.batch_insert(i%k, "Value " + std::to_string(i));
            data_structure.finalize_batch_insert();
        }

        // Create a random number generator
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, n-1);
        std::vector<int> keys;
        // Test for n finds
        for (int i = 0; i < n; ++i) {
            keys.push_back(dis(gen));
        }

        if(!data_structure.supports_multi_get) {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (int i = 0; i < n; ++i) {
                data_structure.get(keys[i]);
            }
            stop = std::chrono::high_resolution_clock::now();
        } else {
            start = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < n; ++i) {
                data_structure.multi_get(keys);
            }
            stop = std::chrono::high_resolution_clock::now();
        }

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::string name = typeid(DataStructure).name();
        int status;
        char* demangled_name = abi::__cxa_demangle(name.c_str(), 0, 0, &status);
        demangled_name = strtok(demangled_name, "<");
        std::cout << "Time taken by " << demangled_name << " for " << n << " finds: " << duration.count() << " microseconds" << std::endl;
        return;
    }

    void test_delete_latency(int n = 1000000, int num_threads = 1) {
        int k = 1000000;
        DataStructure data_structure;
        std::string name = typeid(DataStructure).name();
        int status;
        char* demangled_name = abi::__cxa_demangle(name.c_str(), 0, 0, &status);
        demangled_name = strtok(demangled_name, "<");
        // set omp number of threads
        auto start = std::chrono::high_resolution_clock::now();
        auto stop = std::chrono::high_resolution_clock::now();
        if (!data_structure.supports_batch_insert) {
            #pragma omp parallel for num_threads(num_threads)
            for (uint64_t i = 0; i < n; ++i)
                data_structure.insert(i%k, "Value " + std::to_string(i));
            
        } else {
            #pragma omp parallel for num_threads(num_threads)
            for (uint64_t i = 0; i < n; ++i) 
                data_structure.batch_insert(i%k, "Value " + std::to_string(i));
            data_structure.finalize_batch_insert();
        }

        // Perform n deletes
        if (!data_structure.supports_batch_delete) {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (uint64_t i = 0; i < n; ++i)
                data_structure.remove(i%k);
            stop = std::chrono::high_resolution_clock::now();
        } else {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (uint64_t i = 0; i < n; ++i)
                data_structure.batch_delete(i%k);
            data_structure.finalize_batch_delete();
            stop = std::chrono::high_resolution_clock::now();
        }

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << "Time taken by " << demangled_name << " for " << n << " deletes: " << duration.count() << " microseconds" << std::endl;

        // Confirm that the data structure is empty
        for (int i = 0; i < n; ++i) {
            if(data_structure.get(i%k) != "") {
                std::cout << "Error: Key " << i%k << " not deleted" << std::endl;
            }
        }
        return;
    }

};



int main() {

    int max_threads = std::thread::hardware_concurrency();
    std::cout << "Number of threads: " << max_threads << std::endl;

    BatchParallelMapTest<TBBConcurrentHashMap<uint64_t, std::string>> tbb_test;
    tbb_test.test_ycsb_file("/home/hice1/bthotti3/scratch/uniform/loada_unif_int.dat", "/home/hice1/bthotti3/scratch/uniform/txnsa_unif_int.dat", 16);

    // BatchParallelMapTest<BatchParallelConcurrentHashMap<uint64_t, std::string>> test0;
    // test0.test_ycsb_file("/home/hice1/bthotti3/scratch/uniform/loada_unif_int.dat", "/home/hice1/bthotti3/scratch/uniform/txnsa_unif_int.dat", max_threads);
    // cout<<"****************************"<<endl;
    // BatchParallelMapTest<PAMConcurrentHashMap<uint64_t, std::string>> pam_test;
    // pam_test.test_ycsb_file("/home/hice1/bthotti3/scratch/uniform/loada_unif_int.dat", "/home/hice1/bthotti3/scratch/uniform/txnsa_unif_int.dat", max_threads);

    //BatchParallelMapTest<BatchParallelConcurrentHashMap<int, std::string>> test;
    //  test.test_insert_latency(1000, max_threads);
    //  test.test_insert_latency(10000, max_threads);
    //  test.test_insert_latency(100000, max_threads);
    //  test.test_insert_latency(500000, max_threads);
    //  test.test_insert_latency(1000000, max_threads);
    //  test.test_insert_latency(5000000, max_threads);
    //  test.test_insert_latency(10000000, max_threads);
    //  test.test_insert_latency(50000000, max_threads);
    //  test.test_insert_latency(100000000, max_threads);


    //BatchParallelMapTest<PAMConcurrentHashMap<int, std::string>> test2;
    // test2.test_insert_latency(1000, max_threads);
    // test2.test_insert_latency(10000, max_threads);
    // test2.test_insert_latency(100000, max_threads);
    // test2.test_insert_latency(500000, max_threads);
    //test2.test_insert_latency(1000000, max_threads);
    // test2.test_insert_latency(5000000, max_threads);
    // test2.test_insert_latency(10000000, max_threads);
    // test2.test_insert_latency(50000000, max_threads);
    // test2.test_insert_latency(100000000, max_threads);

    //BatchParallelMapTest<BatchParallelConcurrentHashMap<int, std::string>> test3;
    // test3.test_batch_insert_latency(1000000, 1000, max_threads);
    // test3.test_batch_insert_latency(1000000, 10000, max_threads);
    // test3.test_batch_insert_latency(1000000, 100000, max_threads);
    // test3.test_batch_insert_latency(1000000, 1000000, max_threads);

    // test3.test_batch_insert_latency(10000000, 1000, max_threads);
    // test3.test_batch_insert_latency(10000000, 10000, max_threads);
    // test3.test_batch_insert_latency(10000000, 100000, max_threads);
    // test3.test_batch_insert_latency(10000000, 1000000, max_threads);
    // test3.test_batch_insert_latency(10000000, 10000000, max_threads);

    // test3.test_batch_insert_latency(10000000, 1000, max_threads);
    // test3.test_batch_insert_latency(10000000, 10000, max_threads);
    // test3.test_batch_insert_latency(10000000, 100000, max_threads);
    // test3.test_batch_insert_latency(10000000, 1000000, max_threads);
    // test3.test_batch_insert_latency(10000000, 10000000, max_threads);

    // BatchParallelMapTest<PAMConcurrentHashMap<int, std::string>> test4;
    // test4.test_find_latency(1000000, 1000, max_threads);

    // BatchParallelMapTest<BatchParallelConcurrentHashMap<int, std::string>> test5;
    // // There is no batch parallel find
    // test5.test_find_latency(1000000, 1000, max_threads);

    // BatchParallelMapTest<BatchParallelConcurrentHashMap<int, std::string>> test6;
    // test6.test_delete_latency(1000000, max_threads);

    // BatchParallelMapTest<PAMConcurrentHashMap<int, std::string>> test7;
    // test7.test_delete_latency(1000000, max_threads);
    return 0;

}

#endif // BATCH_PARALLEL_MAP_HPP
