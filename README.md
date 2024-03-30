# CSE-6230-Project
Lazily Batched Parallel Execution Framework for Concurrent Data Structures in C++

Currently I made use of openMP for parellalization and intel TBB concurrent_unordered_set for the underlying concurrent datastructure. 

**login to PACE cluster:**
  ssh <user_name>@login-ice.pace.gatech.edu
  
**load Intel TBB modules onto the node:**
  module load intel/20.0.4
  module load intel-tbb/2020.3

_I used these specific versions as they were mentioned in the PACE demo(https://gatech.service-now.com/home?id=kb_article_view&sysparm_article=KB0041542) but we should try to use newer versions as they have more optimized data structures._

compile the .cpp program, there are 2 cpp files, main.cpp which is just to test if the customizedSet.hpp is working, and then there is time.cpp where I'm calculating basic efficiency of customizedSet over std::set. For compilation I prefer using icc over g++ as icc probably has intel specific optimizations for its data structures library.

icc main.cpp -ltbb -fopenmp -o main
icc time.cpp -ltbb -fopenmp -o time

Execute:
./main
./time (this will print out the timings, example below)

sample timimgs recorded on a 1 node, 4 tasks-per-node machine.
CustomizedSet Insertion Time: 2.35194 ms
CustomizedSet Find Time: 36.6622 ms
CustomizedSet Total Time: 39.0142 ms

std::set Insertion Time: 127.772 ms
std::set Find Time: 0.02263 ms
std::set Total Time: 127.794 ms

Customized set is more efficient :)
