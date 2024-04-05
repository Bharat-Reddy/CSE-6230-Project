#ifndef BATCH_PARALLEL_MAP_HPP
#define BATCH_PARALLEL_MAP_HPP

#include <vector>
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
    std::mutex insertMutex, deleteMutex;
    int approx_size_bound = 10000;
    int dequeue_buffer_size = approx_size_bound * 2;

public:
    bool supports_batch_insert = true;

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

    void finalize_batch_insert() {
        std::pair<Key, Value>* tmp = new std::pair<Key, Value>[dequeue_buffer_size];
        size_t count = insert_queue.try_dequeue_bulk(tmp, dequeue_buffer_size);
        std::vector<std::pair<Key, Value>> batch(tmp, tmp + count);
        std::lock_guard<std::mutex> guard(insertMutex);
        map_ = map_.multi_insert(map_, batch);
    }
    
    void insert(const Key& key, const Value& value) {
        map_.insert(std::make_pair(key, value));
    }

    void remove(const Key& key) {
        map_.remove(key);
    }

    Value get(const Key& key) const {
        auto it = map_.find(key);
        if(it.has_value()) {
            return it.value();
        }
        return Value{};
    }

    Value* multi_get(const std::vector<Key>& keys) const {
        std::copy(keys.begin(), keys.end(), keys);
        auto it = map_.multi_find(map_, keys);
        return it;
    }

};

template<typename Key, typename Value>
class PAMConcurrentHashMap {
    private:
        pam_map<entry<Key, Value>> map_;

    public:
        bool supports_batch_insert = false;

        void insert(const Key& key, const Value& value) {
            map_.insert(std::make_pair(key, value));
        }


        void finalize_batch_insert() {
            // Do nothing
        }

        void batch_insert(const Key& key, const Value& value) {
            map_.insert(std::make_pair(key, value));
        }

        void remove(const Key& key) {
            map_.remove(key);
        }

        Value get(const Key& key) const {
            auto it = map_.find(key);
            if(it.has_value()) {
                return it.value();
            }
            return Value{};
        }
    
        Value* multi_get(const std::vector<Key>& keys) const {
            // Key* tmp = new Key[keys.size()];
            // std::copy(keys.begin(), keys.end(), tmp);
            // auto it = map_.multi_find(map_, tmp);
            // return it;
            return nullptr;
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
        DataStructure data_structure;
        // set omp number of threads
        auto start = std::chrono::high_resolution_clock::now();
        auto stop = std::chrono::high_resolution_clock::now();
        if (!data_structure.supports_batch_insert) {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (int i = 0; i < n; ++i)
                data_structure.insert(i, "Value " + std::to_string(i));
            stop = std::chrono::high_resolution_clock::now();
            
        } else {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for num_threads(num_threads)
            for (int i = 0; i < n; ++i) 
                data_structure.batch_insert(i, "Value " + std::to_string(i));
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
        std::cout << "Value for key " << n-1 << ": " << data_structure.get(n-1) << std::endl;

        // check for correctness
        // std::vector<int> keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        // std::string* values = data_structure.multi_get(keys);
        // for (int i = 0; i < keys.size(); ++i) {
        //     std::cout << "Value for key " << keys[i] << ": " << values[i] << std::endl;
        // }

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
};



int main() {
    // pam_seq<entry<int, std::string>> seq_map;
    // BatchParallelConcurrentHashMap<int, std::string> hash_table;
    // // Create a batch of key-value pairs to insert
    // const int max_hash_table_entries = 100;
    // auto start = std::chrono::high_resolution_clock::now();
    // int num_inserts = 100000;
    // #pragma omp parallel for
    // for (int i = 0; i < num_inserts; ++i) {
    //     hash_table.batch_insert(i%max_hash_table_entries, "Value " + std::to_string(i));
    // }
    // auto stop = std::chrono::high_resolution_clock::now();
    // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    // std::cout << "Time taken by BatchParallelConcurrentHashMap for " << num_inserts << " inserts: " << duration.count() << " microseconds" << std::endl;
    // std::cout << "Value for key 10: " << hash_table.get(10) << std::endl;
    // start = std::chrono::high_resolution_clock::now();
    // PAMConcurrentHashMap<int, std::string> concurrent_hash_table;
    // start = std::chrono::high_resolution_clock::now();
    // #pragma omp parallel for
    // for (int i = 0; i < num_inserts; ++i) {
    //     concurrent_hash_table.insert(i%max_hash_table_entries, "Value " + std::to_string(i));
    // }
    // // Retrieve a value given a key
    // stop = std::chrono::high_resolution_clock::now();
    // duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    // std::cout << "Time taken by PAMConcurrentHashMap for " << num_inserts << " inserts: " << duration.count() << " microseconds" << std::endl;
    // std::cout << "Value for key 10: " << concurrent_hash_table.get(10) << std::endl;
    // int num_deletes = 100000;
    // #pragma omp parallel for
    // for (int i = 0; i < num_deletes; ++i) {
    //     hash_table.batch_delete(i%max_hash_table_entries);
    // }
    // stop = std::chrono::high_resolution_clock::now();
    // duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    // std::cout << "Time taken by BatchParallelConcurrentHashMap for " << num_deletes << " deletes: " << duration.count() << " microseconds" << std::endl;
    // std::cout << "Value for key 10: " << hash_table.get(10) << std::endl;
    /*
    #pragma omp parallel for
    for (int i = 0; i < num_deletes; ++i) {
        concurrent_hash_table.remove(i%max_hash_table_entries,"Value " + std::to_string(i));
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Time taken by PAMConcurrentHashMap for " << num_deletes << " deletes: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Value for key 10: " << hash_table.get(10) << std::endl;
    */

    int max_threads = std::thread::hardware_concurrency();
    std::cout << "Number of threads: " << max_threads << std::endl;
    BatchParallelMapTest<BatchParallelConcurrentHashMap<int, std::string>> test;
    test.test_insert_latency(1000, max_threads);
    test.test_insert_latency(10000, max_threads);
    test.test_insert_latency(100000, max_threads);
    test.test_insert_latency(500000, max_threads);
    test.test_insert_latency(1000000, max_threads);
    test.test_insert_latency(5000000, max_threads);

    BatchParallelMapTest<PAMConcurrentHashMap<int, std::string>> test2;
    test2.test_insert_latency(1000, max_threads);
    test2.test_insert_latency(10000, max_threads);
    test2.test_insert_latency(100000, max_threads);
    test2.test_insert_latency(500000, max_threads);
    test2.test_insert_latency(1000000, max_threads);
    test2.test_insert_latency(5000000, max_threads);

    BatchParallelMapTest<BatchParallelConcurrentHashMap<int, std::string>> test3;
    test3.test_batch_insert_latency(1000000, 1000, max_threads);
    test3.test_batch_insert_latency(1000000, 10000, max_threads);
    test3.test_batch_insert_latency(1000000, 100000, max_threads);
    test3.test_batch_insert_latency(1000000, 1000000, max_threads);

    test3.test_batch_insert_latency(10000000, 1000, max_threads);
    test3.test_batch_insert_latency(10000000, 10000, max_threads);
    test3.test_batch_insert_latency(10000000, 100000, max_threads);
    test3.test_batch_insert_latency(10000000, 1000000, max_threads);
    test3.test_batch_insert_latency(10000000, 10000000, max_threads);
    return 0;

}

#endif // BATCH_PARALLEL_MAP_HPP