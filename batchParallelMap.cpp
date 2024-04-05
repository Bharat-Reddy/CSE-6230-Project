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
// #include "parlay.h"

template<typename Key, typename Value>
struct entry {
using key_t = Key;
using val_t = Value;
static bool comp(key_t a, key_t b) { 
    return a < b;}
};

// struct entry {
//     // int key;
//     using key_t = int;
//     // std::string value;
//     using val_t = std::string;
//     static bool comp(key_t a, key_t b) { 
//         return a < b;}
// };



template<typename Key, typename Value, typename Hash = std::hash<Key>>
class BatchParallelHashMap {
private:
    std::vector<std::unordered_map<Key, Value, Hash>> buckets;
    std::vector<std::pair<std::pair<Key, Value>, int>> batch; 
    // std::vector<std::thread> threads;
    threadpool pool;
    // std::vector<std::mutex> locks;
    // Use array of mutexes instead of vector to avoid resizing
    std::mutex *locks;

    int num_buckets;
    int batch_size;
    int thread_batch_size;
    
    int num_threads = std::thread::hardware_concurrency();

    // Hash function to determine bucket index
    int hash(const Key& key) const {
        return Hash{}(key) % num_buckets;
    }

public:
    // Constructor
    BatchParallelHashMap(int num_buckets, int batch_size) : num_buckets(num_buckets), batch_size(batch_size) {
        buckets.resize(num_buckets);
        // locks.resize(num_buckets);
        locks = new std::mutex[num_buckets];
        thread_batch_size = batch_size / num_threads;
        // threads.reserve(num_threads);
        // pool = threadpool(num_threads);
    }

    // Batch insert function
    void batch_insert(const std::vector<std::pair<Key, Value>>& batch) {
        int batch_count = batch.size();
        int num_threads = std::thread::hardware_concurrency();

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&, i] {
                int start = i * batch_size;
                int end = std::min((i + 1) * batch_size, batch_count);
                for (int j = start; j < end; ++j) {
                    const auto& kv = batch[j];
                    int index = hash(kv.first);
                    std::lock_guard<std::mutex> lock(locks[index]);
                    buckets[index][kv.first] = kv.second;
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }
    }

    void batch_op(const Key& key, const Value& value) {
        int index = hash(key);
        batch.push_back(std::make_pair(std::make_pair(key, value), index));

        if (batch.size() == batch_size) {
            // Let the threads in the pool process the batch
            std::vector<std::pair<std::pair<Key, Value>, int>> batch_copy;
            batch_copy.swap(batch);

            pool.enqueue([&, batch_copy] {
                for (const auto& kv : batch_copy) {
                    int index = kv.second;
                    std::lock_guard<std::mutex> lock(locks[index]);
                    buckets[index][kv.first.first] = kv.first.second;
                }
            });
        }
    }


    // Retrieve value given a key
    Value get(const Key& key) {
        int index = hash(key);
        std::lock_guard<std::mutex> lock(locks[index]);
        auto it = buckets[index].find(key);
        if (it != buckets[index].end()) {
            return it->second;
        }
        // Return a default-constructed value if key not found
        return Value{};
    }
};


template<typename Key, typename Value>
class BatchParallelConcurrentHashMap {
private:

    pam_map<entry<Key, Value>> map_;
    std::vector<std::pair<Key, Value>> batch;
    threadpool pool;
    // lock for batch
    mutable std::mutex mutex;

public:
    bool supports_batch_insert = true;

    void batch_insert(const std::vector<std::pair<Key, Value>>& batch) {
        map_ = map_.multi_insert(map_, batch);
    }

    void insert(const Key& key, const Value& value) {
        map_.insert(std::make_pair(key, value));
    }

    void batch_op(const Key& key, const Value& value) {
        // acquire lock
        std::lock_guard<std::mutex> lock(mutex);
        batch.emplace_back(std::make_pair(key, value));

        if (batch.size() == 10000) {
            // Let some background thread process the batch
            // std::vector<std::pair<Key, Value>> batch_copy;
            // batch_copy.swap(batch);

            // pool.enqueue([&, batch_copy] {
            //     map_ = map_.multi_insert(map_, batch_copy); // 5 seconds
            // }); 
            map_ = map_.multi_insert(map_, batch);
            // map_.multi_find
            batch.clear();
        }
    }

    Value get(const Key& key) const {
        auto it = map_.find(key);
        if(it.has_value()) {
            return it.value();
        }
        return Value{};
    }

    Value* multi_get(const std::vector<Key>& keys) const {
        // return map_.multi_find(map_, parley::make_slice(keys));
        // return map_.multi_find(map_, keys);
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

        void batch_op(const Key& key, const Value& value) {
            map_.insert(std::make_pair(key, value));
        }

        void batch_insert(const std::vector<std::pair<Key, Value>>& batch) {
            map_ = map_.multi_insert(map_, batch);
            // map_.multi_delete();
        }

        Value get(const Key& key) const {
            auto it = map_.find(key);
            if(it.has_value()) {
                return it.value();
            }
            return Value{};
        }

        Value* multi_get(const std::vector<Key>& keys) const {
            // parlay::internal::sample_sort(keys, entry<Key, Value>::comp);
            // return map_.multi_find(map_, parley::make_slice(keys));
            // return map_.multi_find(map_, keys);
        }
};

template<typename Key, typename Value>
class ConcurrentHashMap {
    private:
        std::unordered_map<Key, Value> map_;
        mutable std::mutex mutex_;

    public:
        void insert(const Key& key, const Value& value) {
            std::lock_guard<std::mutex> lock(mutex_);
            map_[key] = value;
        }

        Value get(const Key& key) const {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = map_.find(key);
            if(it != map_.end()) {
                return it->second;
            }
            return Value{};
        }
};

template<typename Key, typename Value>
class BatchParallelVector {
private:
    std::vector<std::pair<Key, Value>> vec;
    std::vector<std::pair<Key, Value>> batch;
    mutable std::mutex mutex;
    int batch_size;

public:
    BatchParallelVector(int batch_size) : batch_size(batch_size) {}

    void batch_insert(const std::vector<std::pair<Key, Value>>& batch) {
        vec.insert(vec.end(), batch.begin(), batch.end());
    }

    void batch_op(const Key& key, const Value& value) {
        batch.push_back(std::make_pair(key, value));

        if (batch.size() == batch_size) {
            // Let some background thread process the batch
            std::vector<std::pair<Key, Value>> batch_copy;
            batch_copy.swap(batch);

            std::thread([&, batch_copy] {
                std::lock_guard<std::mutex> lock(mutex);
                vec.insert(vec.end(), batch_copy.begin(), batch_copy.end());
            }).detach();
        }
    }

    Value get(const Key& key) {
        auto it = std::find_if(vec.begin(), vec.end(), [&](const std::pair<Key, Value>& kv) {
            return kv.first == key;
        });
        if (it != vec.end()) {
            return it->second;
        }
        return Value{};
    }
};

template<typename Key, typename Value>
class ConcurrentVector {
private:
    std::vector<std::pair<Key, Value>> vec;
    mutable std::mutex mutex;

public:
    void insert(const Key& key, const Value& value) {
        std::lock_guard<std::mutex> lock(mutex);
        vec.emplace_back(key, value);
    }

    Value get(const Key& key) {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = std::find_if(vec.begin(), vec.end(), [&](const std::pair<Key, Value>& kv) {
            return kv.first == key;
        });
        if (it != vec.end()) {
            return it->second;
        }
        return Value{};
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
    void test_insert_latency(int n = 1000000) {
        DataStructure data_structure;
        auto start = std::chrono::high_resolution_clock::now();
        auto stop = std::chrono::high_resolution_clock::now();
        if (!data_structure.supports_batch_insert) {
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for
            for (int i = 0; i < n; ++i)
                data_structure.insert(i, "Value " + std::to_string(i));
            stop = std::chrono::high_resolution_clock::now();
            
        } else {
            std::cout << "Batch insert" << std::endl;
            start = std::chrono::high_resolution_clock::now();
            #pragma omp parallel for
            for (int i = 0; i < n; ++i) 
                data_structure.batch_op(i, "Value " + std::to_string(i));
            stop = std::chrono::high_resolution_clock::now();
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        // Time take by DataStructure for n inserts: 0 microseconds
        // Print the data structure name using typeid
        std::cout << "Time taken by " << typeid(DataStructure).name() << " for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
        // Get value for key 10
        std::cout << "Value for key 10: " << data_structure.get(10) << std::endl;
        // std::vector<int> keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        // std::string* values = data_structure.multi_get(keys);
        // for (int i = 0; i < keys.size(); ++i) {
        //     std::cout << "Value for key " << keys[i] << ": " << values[i] << std::endl;
        // }
    }
};


int main() {

    // Use the test class to test the latency of each data structure
    BatchParallelMapTest<BatchParallelConcurrentHashMap<int, std::string>> test;
    // test.test_insert_latency(1000000);
    // test.test_insert_latency(10000000);
    // test.test_insert_latency(100000000);

    BatchParallelMapTest<PAMConcurrentHashMap<int, std::string>> test2;
    test2.test_insert_latency(1000000);
    test2.test_insert_latency(10000000);
    // test2.test_insert_latency(100000000);

    // BatchParallelMapTest<BatchParallelHashMap<int, std::string>> test3;
    // test3.test_insert_latency(1000000);
    // test3.test_insert_latency(10000000);
    // test3.test_insert_latency(100000000);

    // BatchParallelMapTest<ConcurrentHashMap<int, std::string>> test4;
    // test4.test_insert_latency(1000000);
    // test4.test_insert_latency(10000000);
    // test4.test_insert_latency(100000000);

    // BatchParallelMapTest<BatchParallelVector<int, std::string>> test5;
    // test5.test_insert_latency(1000000);
    // test5.test_insert_latency(10000000);
    // test5.test_insert_latency(100000000);

    // BatchParallelMapTest<ConcurrentVector<int, std::string>> test6;
    // test6.test_insert_latency(1000000);
    // test6.test_insert_latency(10000000);
    // test6.test_insert_latency(100000000);

    return 0;
}

#endif // BATCH_PARALLEL_MAP_HPP