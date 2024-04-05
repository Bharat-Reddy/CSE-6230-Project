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

public:
    void batch_insert(const std::vector<std::pair<Key, Value>>& batch) {
        // #pragma omp parallel for
        // for (int i = 0; i < batch.size(); ++i) {
        //     const auto& kv = batch[i];
        //     map_.insert(kv);
        // }
        map_.multi_insert(map_, batch);
    }

    void batch_op(const Key& key, const Value& value) {
        batch.emplace_back(std::make_pair(key, value));

        if (batch.size() == 100) {
            // Let some background thread process the batch
            std::vector<std::pair<Key, Value>> batch_copy;
            batch_copy.swap(batch);

            std::thread([&, batch_copy] {
                // #pragma omp parallel for
                // for (int i = 0; i < batch_copy.size(); ++i) {
                //     const auto& kv = batch_copy[i];
                //     map_.insert(kv);
                // }
                map_.multi_insert(map_, batch);
                // map_.multi_insert(batch);
            }).detach();
        }
    }

    Value get(const Key& key) const {
        auto it = map_.find(key);
        if(it.has_value()) {
            return it.value();
        }
        return Value{};
    }
};

template<typename Key, typename Value>
class PAMConcurrentHashMap {
    private:
        pam_map<entry<Key, Value>> map_;

    public:
        void insert(const Key& key, const Value& value) {
            map_.insert(std::make_pair(key, value));
            // map_.multi_insert
        }

        Value get(const Key& key) const {
            auto it = map_.find(key);
            if(it.has_value()) {
                return it.value();
            }
            return Value{};
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

int main() {

    pam_seq<entry<int, std::string>> seq_map;
    seq_map.insert(std::make_pair(1, "Value 1"));

    // Start the clock
    BatchParallelConcurrentHashMap<int, std::string> hash_table;
    // Create a batch of key-value pairs to insert
    std::vector<std::pair<int, std::string>> batch;
    auto start = std::chrono::high_resolution_clock::now();
    int n = 0;
    for (int i = 0; i < 10; ++i) {
        for (int i = 0; i < 100000; ++i) {
            batch.emplace_back(i, "Value " + std::to_string(i));
            // hash_table.batch_op(i, "Value " + std::to_string(i));
            n++;
        }
    }
    // Insert the batch in parallel
    hash_table.batch_insert(batch);
    // Retrieve a value given a key
    // Stop the clock
    auto stop = std::chrono::high_resolution_clock::now();
    // Calculate the duration
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    // std::cout << "Time taken by BatchParallelConcurrentHashMap: " << duration.count() << " microseconds" << std::endl;
    // Time take by BatchParallelConcurrentHashMap for n inserts: 0 microseconds
    std::cout << "Time taken by BatchParallelConcurrentHashMap for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Value for key 10: " << hash_table.get(10) << std::endl;


    PAMConcurrentHashMap<int, std::string> concurrent_hash_table;
    n = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 10; ++i) {
        #pragma omp parallel for
        for (int i = 0; i < 100000; ++i) {
            concurrent_hash_table.insert(i, "Value " + std::to_string(i));
            n++;
        }
    }
    // Retrieve a value given a key
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Time taken by PAMConcurrentHashMap for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Value for key 10: " << concurrent_hash_table.get(10) << std::endl;


    BatchParallelHashMap<int, std::string> hash_map(10, 1000);
    batch.clear();
    n = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 10; ++i) {
        #pragma omp parallel for
        for (int i = 0; i < 10000000; ++i) {
            hash_map.batch_op(i, "Value " + std::to_string(i));
            n++;
        }
    }

    // hash_map.batch_insert(batch);
    std::string value = hash_map.get(10);
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Time taken by BatchParallelHashMap for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Value for key 10: " << value << std::endl;


    ConcurrentHashMap<int, std::string> concurrent_map;
    n = 0;
    start = std::chrono::high_resolution_clock::now();
    #pragma omp parallel for
    for (int i = 0; i < 100000000; ++i) {
        concurrent_map.insert(i, "Value " + std::to_string(i));
        n++;
    }
    value = hash_map.get(10);
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Time taken by ConcurrentHashMap for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Value for key 10: " << concurrent_map.get(10) << std::endl;

    BatchParallelVector<int, std::string> vector(100000);
    batch.clear();
    start = std::chrono::high_resolution_clock::now();
    n = 0;
    for (int i = 0; i < 10; ++i) {
        for (int i = 0; i < 1000000; ++i) {
            batch.emplace_back(i, "Value " + std::to_string(i));
            n++;
        }
        vector.batch_insert(batch);
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Time taken by BatchParallelVector for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Value for key 10: " << vector.get(10) << std::endl;

    ConcurrentVector<int, std::string> concurrent_vector;
    n = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 10; ++i) {
        #pragma omp parallel for
        for (int i = 0; i < 1000000; ++i) {
            concurrent_vector.insert(i, "Value " + std::to_string(i));
            n++;
        }
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Time taken by ConcurrentVector for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Value for key 10: " << concurrent_vector.get(10) << std::endl;

    return 0;
}

#endif // BATCH_PARALLEL_MAP_HPP