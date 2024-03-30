#include "customizedSet.hpp"
#include <chrono>
#include <iostream>
#include <set>

int main() {
    CustomizedSet<int> myCustomSet;
    std::set<int> myStdSet;

    // Start time for CustomizedSet insertions
    auto customStart = std::chrono::high_resolution_clock::now();

    // Insert 1 million elements into CustomizedSet
    for(int i = 0; i < 1000000; ++i) {
        myCustomSet.insert(i);
    }

    // Find in CustomizedSet
    auto customFindStart = std::chrono::high_resolution_clock::now();
    if (myCustomSet.find(500000) != myCustomSet.end()) {
        std::cout << "CustomizedSet: Found 500000" << std::endl;
    } else {
        std::cout << "CustomizedSet: ERROR - Not found 500000" << std::endl;
    }
    auto customFindEnd = std::chrono::high_resolution_clock::now();

    // End time for CustomizedSet operations
    auto customEnd = std::chrono::high_resolution_clock::now();

    // Start time for std::set insertions
    auto stdStart = std::chrono::high_resolution_clock::now();

    // Insert 1 million elements into std::set
    for(int i = 0; i < 1000000; ++i) {
        myStdSet.insert(i);
    }

    // Find in std::set
    auto stdFindStart = std::chrono::high_resolution_clock::now();
    if (myStdSet.find(500000) != myStdSet.end()) {
        std::cout << "std::set: Found 500000" << std::endl;
    } else {
        std::cout << "std::set: ERROR - Not found 500000" << std::endl;
    }
    auto stdFindEnd = std::chrono::high_resolution_clock::now();

    // End time for std::set operations
    auto stdEnd = std::chrono::high_resolution_clock::now();

    // Calculate durations for CustomizedSet
    std::chrono::duration<double, std::milli> customInsertTime = customFindStart - customStart;
    std::chrono::duration<double, std::milli> customFindTime = customFindEnd - customFindStart;
    std::chrono::duration<double, std::milli> customTotalTime = customEnd - customStart;

    // Calculate durations for std::set
    std::chrono::duration<double, std::milli> stdInsertTime = stdFindStart - stdStart;
    std::chrono::duration<double, std::milli> stdFindTime = stdFindEnd - stdFindStart;
    std::chrono::duration<double, std::milli> stdTotalTime = stdEnd - stdStart;

    // Print results for CustomizedSet
    std::cout << "CustomizedSet Insertion Time: " << customInsertTime.count() << " ms" << std::endl;
    std::cout << "CustomizedSet Find Time: " << customFindTime.count() << " ms" << std::endl;
    std::cout << "CustomizedSet Total Time: " << customTotalTime.count() << " ms" << std::endl;

    // Print results for std::set
    std::cout << "std::set Insertion Time: " << stdInsertTime.count() << " ms" << std::endl;
    std::cout << "std::set Find Time: " << stdFindTime.count() << " ms" << std::endl;
    std::cout << "std::set Total Time: " << stdTotalTime.count() << " ms" << std::endl;

    return 0;
}

