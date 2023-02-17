#include <iostream>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include <stdio.h>
#include <vector>
#include "CCEH.h"
#include "hash.h"
#include "util.h"

#define f_seed 0xc70697UL
#define s_seed 0xc70697UL
//#define f_seed 0xc70f6907UL
//#define s_seed 0xc70f6907UL

using namespace std;

bool dir_read(Directory *dir) {
    ifstream file;
    file.open("data/Dir", ios::binary | ios::in);
    if (!file) return false;
    file.read(reinterpret_cast<char *>(dir), sizeof(Directory));
    file.close();
    return true;
}

void dir_write(Directory *dir) {
    ofstream file;
    file.open("data/Dir", ios::binary | ios::out);
    file.write(reinterpret_cast<const char *>(dir), sizeof(Directory));
    file.close();
}

bool segment_read(size_t x, Segment *target) {
    ifstream file;
    string file_no = "data/" + to_string(x);
    file.open(file_no, ios::binary | ios::in);
    if (!file) return false;
    file.read(reinterpret_cast<char *>(target), sizeof(Segment));
    file.close();
    return true;
}

bool segment_write(size_t x, Segment *target) {
    ofstream file;
    string file_no = "data/" + to_string(x);
    file.open(file_no, ios::binary | ios::out);
    if (!file) return 0;
    file.write(reinterpret_cast<const char *>(target), sizeof(Segment));
    file.close();
    return 1;
}

bool Segment::Insert4split(Key_t &key, Value_t value, size_t loc) {
    for (int i = 0; i < kNumPairPerCacheLine; ++i) {
        auto slot = (loc + i) % kNumSlot;
        if (bucket[slot].key == INVALID) {
            bucket[slot].key = key;
            bucket[slot].value = value;
            return true;
        }
    }
    return false;
}

struct Segment *Segment::Split(size_t segment_num) {
    struct Segment *split = new struct Segment;
    split->initSegment(local_depth + 1);
    auto pattern = ((size_t) 1 << (sizeof(Key_t) * 8 - local_depth - 1));
    for (int i = 0; i < kNumSlot; ++i) {
        auto f_hash = hash_funcs[0](&bucket[i].key, sizeof(Key_t), f_seed);
        if (f_hash & pattern) {
            if (!split->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask) * kNumPairPerCacheLine)) {
                auto s_hash = hash_funcs[2](&bucket[i].key, sizeof(Key_t), s_seed);
                if (!split->Insert4split(bucket[i].key, bucket[i].value, (s_hash & kMask) * kNumPairPerCacheLine)) {

                }
            }
        }
    }
    segment_write(segment_num, split);
    return split;
}

void CCEH::initCCEH() {
    crashed = true;
    dir = new struct Directory;
    dir->initDirectory();
    auto segment = new struct Segment;
    segment->initSegment();
    for (int i = 0; i < dir->capacity; i++) {
        if (!segment_write(dir->segmentPointers[i], segment)) {
            cout << dir->segmentPointers[i] << "号文件打不开" << endl;
            exit(0);
        }
    }
    delete (segment);
}

void CCEH::initCCEH(size_t initCap) {
    crashed = true;
    dir = new struct Directory;
    dir->initDirectory(static_cast<size_t>(log2(initCap)));
    auto segment = new struct Segment;
    segment->initSegment(static_cast<size_t>(log2(initCap)));
    for (int i = 0; i < dir->capacity; i++) {
        if (!segment_write(dir->segmentPointers[i], segment)) {
            cout << dir->segmentPointers[i] << "号文件打不开" << endl;
            exit(0);
        }
    }
    delete (segment);
}

void CCEH::Insert(Key_t &key, Value_t value) {
    auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
    auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;
    auto target = new struct Segment;
    RETRY:
    auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));

    bool flag = segment_read(dir->segmentPointers[x], target);
    if (!flag) {
        std::this_thread::yield();
        goto RETRY;
    }

    if (!target->lock()) {
        std::this_thread::yield();
        goto RETRY;
    }
    segment_write(dir->segmentPointers[x], target);

    auto check_x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    auto target_check = new struct Segment;
    segment_read(dir->segmentPointers[check_x], target_check);
    if (!target->equal(target_check)) {
        target->unlock();
        segment_write(dir->segmentPointers[x], target);
        std::this_thread::yield();
        delete target_check;
        goto RETRY;
    }
    delete target_check;

    auto pattern = (f_hash >> (8 * sizeof(f_hash) - target->local_depth));
    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (f_idx + i) % Segment::kNumSlot;
        auto _key = target->bucket[loc].key;
        /* validity check for entry keys */
        if ((((hash_funcs[0](&target->bucket[loc].key, sizeof(Key_t), f_seed)
                >> (8 * sizeof(f_hash) - target->local_depth)) != pattern) || (target->bucket[loc].key == INVALID)) &&
            (target->bucket[loc].key != SENTINEL)) {
            if (CAS(&target->bucket[loc].key, &_key, SENTINEL)) {
                target->bucket[loc].value = value;
                target->bucket[loc].key = key;
                segment_write(dir->segmentPointers[x], target);
                /* release segment exclusive lock */
                target->unlock();
                segment_write(dir->segmentPointers[x], target);
                delete target;
                return;
            }
        }
    }

    auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
    auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;

    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (s_idx + i) % Segment::kNumSlot;
        auto _key = target->bucket[loc].key;
        if ((((hash_funcs[0](&target->bucket[loc].key, sizeof(Key_t), f_seed)
                >> (8 * sizeof(s_hash) - target->local_depth)) != pattern) ||
             (target->bucket[loc].key == INVALID)) && (target->bucket[loc].key != SENTINEL)) {
            if (CAS(&target->bucket[loc].key, &_key, SENTINEL)) {
                target->bucket[loc].value = value;
                target->bucket[loc].key = key;
                segment_write(dir->segmentPointers[x], target);
                target->unlock();
                segment_write(dir->segmentPointers[x], target);
                delete target;
                return;
            }
        }
    }

    auto target_local_depth = target->local_depth;
    // COLLISION !!
    /* need to split segment but release the exclusive lock first to avoid deadlock */
    target->unlock();
    segment_write(dir->segmentPointers[x], target);
    if (!target->suspend()) {
        segment_write(dir->segmentPointers[x], target);//不太清楚要不要写一次文件
        std::this_thread::yield();
        goto RETRY;
    }
    segment_write(dir->segmentPointers[x], target);//不太清楚要不要写一次文件

    if (target_local_depth != target->local_depth) {
        target->sema = 0;
        segment_write(dir->segmentPointers[x], target);
        std::this_thread::yield();
        goto RETRY;
    }

    target->Split(dir->segment_num++);
    DIR_RETRY:
    if (target->local_depth == dir->depth) {//dir扩容
        if (!dir->suspend()) {
            std::this_thread::yield();
            //不知道要不要写一次文件
            goto DIR_RETRY;
        }
        x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
        dir->depth++;
        dir->capacity <<= 1;
        auto old_segmentPoints = dir->segmentPointers;
        for (int i = 0; i < dir->capacity / 2; ++i) {
            if (i == x) {
                dir->segmentPointers[2 * i] = old_segmentPoints[i];
                dir->segmentPointers[2 * i + 1] = dir->segment_num - 1;//新的segment文件编号
            } else {
                dir->segmentPointers[2 * i] = old_segmentPoints[i];
                dir->segmentPointers[2 * i + 1] = old_segmentPoints[i];
            }
        }
        dir_write(dir);
        target->local_depth++;
        segment_write(old_segmentPoints[x], target);
        target->sema = 0;
    } else {//dir不需要扩容
        while (!dir->lock()) {
            asm("nop");
        }
        auto y = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
        if (dir->depth == target->local_depth + 1) {
            if (y % 2 == 0) {
                dir->segmentPointers[y + 1] = dir->segment_num - 1;
                dir_write(dir);
            } else {
                dir->segmentPointers[y] = dir->segment_num - 1;
                dir_write(dir);
            }
            dir->unlock();
            target->local_depth++;
            segment_write(dir->segmentPointers[x], target);
            target->sema = 0;
        } else {
            int stride = pow(2, dir->depth - target_local_depth);
            auto loc = x - (x % stride);
            for (int i = 0; i < stride / 2; ++i) {
                dir->segmentPointers[loc + stride / 2 + i] = dir->segment_num - 1;
            }
            dir_write(dir);
            dir->unlock();
            target->local_depth++;
            segment_write(dir->segmentPointers[x], target);
            target->sema = 0;
        }
    }
    std::this_thread::yield();
    goto RETRY;
}

bool CCEH::Delete(Key_t &key) {
    return false;
}

Value_t CCEH::Get(Key_t &key) {
    auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
    auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;
    auto target = new struct Segment;
    RETRY:
    while (dir->sema < 0) {
        asm("nop");
    }
    auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));

    bool flag = segment_read(dir->segmentPointers[x], target);
    if (!flag) {
        std::this_thread::yield();
        goto RETRY;
    }

    if (!target->lock()) {
        std::this_thread::yield();
        goto RETRY;
    }
    segment_write(dir->segmentPointers[x], target);

    auto check_x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    auto target_check = new struct Segment;
    segment_read(dir->segmentPointers[check_x], target_check);
    if (!target->equal(target_check)) {
        target->unlock();
        segment_write(dir->segmentPointers[x], target);
        std::this_thread::yield();
        delete target_check;
        goto RETRY;
    }
    delete target_check;

    for (int i = 0; i < kNumPairPerCacheLine; ++i) {
        auto loc = (f_idx + i) % Segment::kNumSlot;
        if (target->bucket[loc].key == key) {
            Value_t v = target->bucket[loc].value;
            target->unlock();//ifdef INPLACE
            delete target;
            return v;
        }
    }

    auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
    auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;
    for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (s_idx + i) % Segment::kNumSlot;
        if (target->bucket[loc].key == key) {
            Value_t v = target->bucket[loc].value;
            target->unlock();//ifdef INPLACE
            delete target;
            return v;
        }
    }
    target->unlock();//ifdef INPLACE
    return NONE;
}

void CCEH::Recovery() {
    dir = new struct Directory;
    bool flag = dir_read(dir);
    if (!flag) {
        std::cout << "dir文件打开失败" << std::endl;
        exit(0);
    }
}

double CCEH::Utilization() {
    size_t sum = 0;
    size_t cnt = 0;
    auto target = new struct Segment;
    for (int i = 0; i < dir->capacity; ++cnt) {
        segment_read(dir->segmentPointers[i], target);
        int stride = pow(2, dir->depth - target->local_depth);
        auto pattern = (i >> (dir->depth - target->local_depth));
        for (unsigned j = 0; j < Segment::kNumSlot; ++j) {
            auto f_hash = h(&target->bucket[j].key, sizeof(Key_t));
            if (((f_hash >> (8 * sizeof(f_hash) - target->local_depth)) == pattern) &&
                (target->bucket[j].key != INVALID)) {
                sum++;
            }
        }
        i += stride;
    }
    delete target;
    return ((double) sum) / ((double) cnt * Segment::kNumSlot) * 100.0;
}

size_t CCEH::Capacity(void) {
    size_t cnt = 0;
    auto target = new struct Segment;
    for (int i = 0; i < dir->capacity; cnt++) {
        segment_read(dir->segmentPointers[i], target);
        int stride = pow(2, dir->depth - target->local_depth);
        i += stride;
    }
    delete target;
    return cnt * Segment::kNumSlot;
}

// for debugging
Value_t CCEH::FindAnyway(Key_t &key) {
    auto target = new struct Segment;
    for (size_t i = 0; i < dir->capacity; ++i) {
        for (size_t j = 0; j < Segment::kNumSlot; ++j) {
            segment_read(dir->segmentPointers[i], target);
            if (target->bucket[j].key == key) {
                cout << "segment(" << i << ")" << endl;
                cout << "global_depth(" << dir->depth << "), local_depth(" << target->local_depth << ")" << endl;
                cout << "pattern: " << bitset<sizeof(int64_t)>(i >> (dir->depth - target->local_depth)) << endl;
                cout << "Key MSB: "
                     << bitset<sizeof(int64_t)>(h(&key, sizeof(key)) >> (8 * sizeof(key) - target->local_depth))
                     << endl;
                return target->bucket[j].value;
            }
        }
    }
    delete target;
    return NONE;
}

