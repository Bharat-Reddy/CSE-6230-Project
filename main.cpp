#include "customizedSet.hpp"

int main() {
    CustomizedSet<int> mySet;
    mySet.insert(10);
    mySet.insert(20);

    if (mySet.find(10) != mySet.end()) {
        std::cout << "Found 10" << std::endl;
    } else {
	std::cout<<"ERROR: Not found 10"<<std::endl;
    }

    mySet.erase(10);

    if (mySet.find(10) == mySet.end()) {
        std::cout << "Didn't find 10" << std::endl;
    } else {
	std::cout<<"ERROR: Found 10"<<std::endl;
    }

    return 0;
}

