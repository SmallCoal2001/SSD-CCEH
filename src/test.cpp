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
#include "timer.h"

const int Numthread = 8;
const int TestNum = 100000;
// 540出现第一个错 919出现第二个错
using namespace std;

CCEH *cceh = new CCEH;

void clear_cache()
{
    int *dummy = new int[1024 * 1024 * 256];
    for (int i = 0; i < 1024 * 1024 * 256; i++)
    {
        dummy[i] = i;
    }

    for (int i = 100; i < 1024 * 1024 * 256 - 100; i++)
    {
        dummy[i] = dummy[i - rand() % 100] + dummy[i + rand() % 100];
    }

    delete[] dummy;
}


int main()
{
    ifstream ifs;
    cceh->initCCEH();
    clear_cache();
    Timer t;
    // for (int i = 0; i < TestNum; i++)
    // {
    //     Key_t key = i;
    //     cceh->Insert(key, reinterpret_cast<Value_t>(i));
    // }
    for (int i = 0; i < Numthread; i++)
    {
        std::async(std::launch::async, [](int tid){
            for(int i=0; i<TestNum/Numthread; i++) {
                Key_t key = i + tid * TestNum;
                cceh->Insert(key, reinterpret_cast<Value_t>(i + tid * TestNum));
            }
        }, i);
    }
    printf("Insert IOPS: %.2f\n", TestNum / t.GetDurationSec());
    clear_cache();
    t.Reset();
    // for (int i = 0; i < TestNum; i++)
    // {
    //     Key_t key = i;
    //     auto t = cceh->Get(key);
    //     // if (!(reinterpret_cast<Value_t>(t) == reinterpret_cast<Value_t>(i))) {
    //     //     cout << i << "出错了" << reinterpret_cast<Key_t>(t) << endl;
    //     // }
    //     LOG_ASSERT(reinterpret_cast<Value_t>(t) == reinterpret_cast<Value_t>(i), "%d", i);
    // }
    for (int i = 0; i < Numthread; i++)
    {
        std::async(std::launch::async, [](int tid){
            for(int i=0; i<TestNum/Numthread; i++) {
                Key_t key = i + tid * TestNum;
                auto t = cceh->Get(key);
                LOG_ASSERT(reinterpret_cast<Value_t>(t) == reinterpret_cast<Value_t>(i + tid * TestNum), "%d", i);
            }
        }, i);
    }
    printf("Read IOPS: %.2f\n", TestNum / t.GetDurationSec());
    return 0;
}
