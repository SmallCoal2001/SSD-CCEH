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

#include "CCEH.h"

using namespace std;

int main() {
    Key_t key = 1000;
    auto cceh = new CCEH;
    cceh->initCCEH();
    cceh->Insert(key, reinterpret_cast<Value_t>(key));
    auto t = cceh->Get(key);
    if (t != reinterpret_cast<Value_t>(key)) {
        cout << 0 << endl;
    }
    cout << 1 << endl;
    return 0;
}
