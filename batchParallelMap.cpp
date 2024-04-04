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
    // std::vector<std::mutex> locks;
    // Use array of mutexes instead of vector to avoid resizing
    std::mutex *locks;

    int num_buckets;
    int batch_size;

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
    }

    // Batch insert function
    void batch_insert(const std::vector<std::pair<Key, Value>>& batch) {
        int batch_count = batch.size();
        int num_threads = (batch_count + batch_size - 1) / batch_size;

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
class BatchParallelConcurrentHashTable {
private:

    pam_map<entry<Key, Value>> map_;

public:
    void batch_insert(const std::vector<std::pair<Key, Value>>& batch) {
        #pragma omp parallel for
        for (int i = 0; i < batch.size(); ++i) {
            const auto& kv = batch[i];
            map_.insert(kv);
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
class ConcurrentHashTable {
    private:
        pam_map<entry<Key, Value>> map_;

    public:
        void insert(const Key& key, const Value& value) {
            map_.insert(std::make_pair(key, value));
        }

        Value get(const Key& key) const {
            auto it = map_.find(key);
            if(it.has_value()) {
                return it.value();
            }
            return Value{};
        }
};

int main() {

    // Start the clock
    BatchParallelConcurrentHashTable<int, std::string> hash_table;
    // Create a batch of key-value pairs to insert
    std::vector<std::pair<int, std::string>> batch;
    auto start = std::chrono::high_resolution_clock::now();
    int n = 0;
    for (int i = 0; i < 10; ++i) {
        for (int i = 0; i < 100000; ++i) {
            batch.emplace_back(i, "Value " + std::to_string(i));
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
    // std::cout << "Time taken by BatchParallelConcurrentHashTable: " << duration.count() << " microseconds" << std::endl;
    // Time take by BatchParallelConcurrentHashTable for n inserts: 0 microseconds
    std::cout << "Time taken by BatchParallelConcurrentHashTable for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Value for key 10: " << hash_table.get(10) << std::endl;


    ConcurrentHashTable<int, std::string> concurrent_hash_table;
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
    std::cout << "Time taken by ConcurrentHashTable for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Value for key 10: " << concurrent_hash_table.get(10) << std::endl;


    BatchParallelHashMap<int, std::string> hash_map(10, 100000);
    batch.clear();
    start = std::chrono::high_resolution_clock::now();
    n = 0;
    for (int i = 0; i < 10; ++i) {
        for (int i = 0; i < 100000; ++i) {
            batch.emplace_back(i, "Value " + std::to_string(i));
            n++;
        }
    }
    hash_map.batch_insert(batch);
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Time taken by BatchParallelHashMap for " << n << " inserts: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Value for key 10: " << hash_map.get(10) << std::endl;

    return 0;
}

#endif // BATCH_PARALLEL_MAP_HPP