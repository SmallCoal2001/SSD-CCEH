#include <cstdio>
#include <ctime>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <thread>
#include <vector>
#include <bitset>
#include <future>
#include <cassert>
#include "Random.h"
#include "CCEH.h"
#include "logging.h"

const int Numthread = 8;
const int TestNum = 1000000;
//540出现第一个错 919出现第二个错
//526753出错
using namespace std;

CCEH *cceh = new CCEH;

int main() {
    cceh->initCCEH();
    // for (int i = 0; i < Numthread; i++)
    // {
    //     std::async(std::launch::async, [](){
    //         for(int i=0; i<TestNum; i++) {
    //             Key_t key = i;
    //             cceh->Insert(key, reinterpret_cast<Value_t>(i));
    //         }
    //     });
    // }


    for (int i = 0; i < TestNum; i++) {
        Key_t key = i;
        cceh->Insert(key, reinterpret_cast<Value_t>(i));
        //cout << key << endl;
    }
    for (int i = 0; i < TestNum; i++) {
        Key_t key = i;
        auto t = cceh->Get(key);
        if (reinterpret_cast<Value_t>(t) != reinterpret_cast<Value_t>(i)) {
            cout << i << "出错了" << reinterpret_cast<Key_t>(t) << endl;
        }
        //LOG_ASSERT(reinterpret_cast<Value_t>(t) == reinterpret_cast<Value_t>(i), "%d", i);
    }
    return 0;
}
