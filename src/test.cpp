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
#include "cmdline.h"

int Numthread;
const int TestNum = 1000000;

using namespace std;

CCEH *cceh = new CCEH;


cmdline::parser pser;

int main(int argc, char *argv[]) {
    pser.add<int>("t", 't', "thread num", true, 1, cmdline::range(1, 100));
    pser.add("help", 0, "print this message");
    bool ok = pser.parse(argc, argv);
    if (argc == 1 || pser.exist("help")) {
        cerr << pser.usage();
        return 0;
    }
    if (!ok) {
        cerr << pser.error() << endl
             << pser.usage();
        return 0;
    }
    Numthread = pser.get<int>("t");
    ifstream ifs;
    cceh->initCCEH();
    Timer t;
    std::vector<std::thread> res;
    for (int i = 0; i < Numthread; i++) {
        res.emplace_back([](int tid) {
                             for (int i = 0; i < TestNum / Numthread; i++) {
                                 Key_t key = i + tid * TestNum;
                                 cceh->Insert(key, reinterpret_cast<Value_t>(i + tid * TestNum));
                             }
                         },
                         i);
    }
    for (auto &t: res)
        t.join();

    printf("Insert IOPS: %.2f\n", TestNum / t.GetDurationSec());
    t.Reset();
    res.clear();

    for (int i = 0; i < Numthread; i++) {
        res.emplace_back([](int tid) {
                             for (int i = 0; i < TestNum / Numthread; i++) {
                                 Key_t key = i + tid * TestNum;
                                 auto t = cceh->Get(key);
                                 if (reinterpret_cast<Value_t>(t) != reinterpret_cast<Value_t>(i + tid * TestNum))
                                     LOG_INFO("%d %d", reinterpret_cast<Value_t>(t), reinterpret_cast<Value_t>(i + tid * TestNum));
                                 LOG_ASSERT(reinterpret_cast<Value_t>(t) == reinterpret_cast<Value_t>(i + tid * TestNum), "%d", i);
                             }
                         },
                         i);
    }
    for (auto &t: res)
        t.join();
    printf("Read IOPS: %.2f\n", TestNum / t.GetDurationSec());
    return 0;
}
