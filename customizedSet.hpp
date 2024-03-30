#pragma once
#include <set>
#include <set>
#include <vector>
#include <algorithm>
#include <iostream>
#include <omp.h>
#include <tbb/concurrent_unordered_set.h>

template<typename T>
class CustomizedSet {
private:
    tbb::concurrent_unordered_set<T> internalSet; //I think the recent versions of intel TBB has concurrent_set, might need to explore that as its more accurate when representing set
    std::vector<T> buffer;

    void flushBuffer() {
        if (buffer.empty()) return;
        
        // Insert elements from buffer to internalSet in parallel
        #pragma omp parallel for //right now using openMP for demo, we should probably use MPI and CUDA for more parallelization??? remember we'll be doing millions of insertions!
        for (int i = 0; i < static_cast<int>(buffer.size()); ++i) {
	    internalSet.insert(buffer[i]);
        }

        buffer.clear(); // Clear the buffer after flushing
    }

public:
    void insert(const T& value) {
        //std::cout << "Inserting: " << value << std::endl;
        buffer.push_back(value);
    }

    typename tbb::concurrent_unordered_set<T>::iterator find(const T& value) {
        flushBuffer();
        //std::cout << "Searching for: " << value << std::endl;
        auto it = internalSet.find(value);
        // Custom logic
        return it;
    }

    size_t erase(const T& value) {
        flushBuffer();
        //std::cout << "Erasing: " << value << std::endl;
        auto result = internalSet.unsafe_erase(value);
        // Custom logic
        //std::cout << (result > 0 ? "Erased." : "Not found.") << std::endl;
        return result;
    }

    typename tbb::interface5::concurrent_unordered_set<T>::const_iterator end() const {
        return internalSet.end();
    }

};

