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

fstream segFile;

//bool dir_read(Directory *dir) {
//    ifstream file;
//    file.open("data/Dir", ios::binary | ios::in);
//    if (!file) return false;
//    file.read(reinterpret_cast<char *>(dir), sizeof(Directory));
//    file.close();
//    return true;
//}
//
//void dir_write(Directory *dir) {
//    ofstream file;
//    file.open("data/Dir", ios::binary | ios::out);
//    file.write(reinterpret_cast<const char *>(dir), sizeof(Directory));
//    file.close();
//}
//
//bool segment_read(size_t x, Segment *target) {
//    ifstream file;
//    string file_no = "data/" + to_string(x);
//    file.open(file_no, ios::binary | ios::in);
//    if (!file) return false;
//    file.read(reinterpret_cast<char *>(target), sizeof(Segment));
//    file.close();
//    return true;
//}
//
//bool segment_write(size_t x, Segment *target) {
//    ofstream file;
//    string file_no = "data/" + to_string(x);
//    file.open(file_no, ios::binary | ios::out);
//    if (!file) return 0;
//    file.write(reinterpret_cast<const char *>(target), sizeof(Segment));
//    file.close();
//    return 1;
//}

void local_depth_read(int index, char *target) {
    segFile.seekg(index * sizeof(Segment) + kSegmentSize, ios::beg);
    segFile.read(target, sizeof(size_t));
}

void local_depth_write(int index, const char *target) {
    segFile.seekp(index * sizeof(Segment) + kSegmentSize, ios::beg);
    segFile.write(target, sizeof(size_t));
}

void bucket_write(int index, int loc, const char *target) {
    segFile.seekp(index * sizeof(Segment) + loc * sizeof(Pair), ios::beg);
    segFile.write(target, sizeof(Pair));
}

void segment_read(int index, Segment *target) {
    segFile.seekg(index * sizeof(Segment), ios::beg);
    segFile.read(reinterpret_cast<char *>(target), sizeof(Segment));
}

void segment_write(int index, Segment *target) {
    segFile.seekp(index * sizeof(Segment), ios::beg);
    segFile.write(reinterpret_cast<const char *>(target), sizeof(Segment));
}


bool Segment::Insert4split(Key_t &key, Value_t value, size_t loc) {
    for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (loc + i) % kNumSlot;
        if (bucket[slot].key == INVALID) {
            //cout << key << "的slot：" << slot << endl;
            bucket[slot].key = key;
            bucket[slot].value = value;
            return true;
        }
    }
    return false;
}

void Segment::Split(size_t index) {
    struct Segment *split = new struct Segment;
    split->initSegment(local_depth + 1);
    //cout << "newsegIndex:" << index << endl;
    auto pattern = ((size_t) 1 << (sizeof(Key_t) * 8 - local_depth - 1));
    for (int i = 0; i < kNumSlot; ++i) {
        auto f_hash = hash_funcs[0](&bucket[i].key, sizeof(Key_t), f_seed);
        if (f_hash & pattern) {
//            cout << bucket[i].key << "进入了Insert4split" << endl;
            split->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask) * kNumPairPerCacheLine);
//            if (!split->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask) * kNumPairPerCacheLine)) {
//                auto s_hash = hash_funcs[2](&bucket[i].key, sizeof(Key_t), s_seed);
//                if (!split->Insert4split(bucket[i].key, bucket[i].value, (s_hash & kMask) * kNumPairPerCacheLine)) {
//
//                }
//            }
        }
    }
    segment_write(index, split);
    delete split;
}

void CCEH::initCCEH() {
    crashed = true;
    dir = new struct Directory;
    dir->initDirectory();
    auto target = new struct Segment;
    target->initSegment(dir->depth);//之前是target->initSegment();
    for (int i = 0; i < dir->capacity; i++) {
        segment_write(dir->segIndex[i], target);
    }
    delete (target);
}

void CCEH::initCCEH(size_t initCap) {
    crashed = true;
    dir = new struct Directory;
    dir->initDirectory(static_cast<size_t>(log2(initCap)));
    auto target = new struct Segment;
    target->initSegment(static_cast<size_t>(log2(initCap)));
    for (int i = 0; i < dir->capacity; i++) {
        segment_write(dir->segIndex[i], target);
    }
    delete (target);
}

void CCEH::Insert(Key_t &key, Value_t value) {
    auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
    auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;
    auto target = new struct Segment;

    RETRY:
    auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    int segIndex = dir->segIndex[x];
    segment_read(segIndex, target);

    if (!dir->lock(segIndex)) {
        std::this_thread::yield();
        goto RETRY;
    }

    auto x_check = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    int segIndex_check = dir->segIndex[x_check];
    auto target_check = new struct Segment;
    segment_read(segIndex_check, target_check);
    if (!target->equal(target_check)) {
        dir->unlock(segIndex);
        delete target_check;
        std::this_thread::yield();
        goto RETRY;
    }
    delete target_check;

    auto pattern = (f_hash >> (8 * sizeof(f_hash) - target->local_depth));
//    if (key == 486486 || key == 507997 || key == 526752 || key == 523346) {
//        cout << dec << key << "的local_depth:" << target->local_depth << endl;
//        cout << dec << key << "的f_hash：" << hex << f_hash << endl;
//        cout << dec << key << "的pattern:" << hex << pattern << endl;
//        cout << dec << key << "的segIndex:" << segIndex << endl;
//        cout << dec << key << "的x:" << x << endl;
//    }
    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (f_idx + i) % Segment::kNumSlot;
        auto _key = target->bucket[loc].key;
        if ((((hash_funcs[0](&target->bucket[loc].key, sizeof(Key_t), f_seed)
                >> (8 * sizeof(f_hash) - target->local_depth)) != pattern) ||
             (target->bucket[loc].key == INVALID)) && (target->bucket[loc].key != SENTINEL)) {
            if (CAS(&target->bucket[loc].key, &_key, SENTINEL)) {
                target->bucket[loc].value = value;
                mfence();
                target->bucket[loc].key = key;
                bucket_write(segIndex, loc, reinterpret_cast<const char *>(&target->bucket[loc]));
                dir->unlock(segIndex);
                delete target;
                return;
            }
        }
    }

//    auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
//    auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;
//
//    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
//        auto loc = (s_idx + i) % Segment::kNumSlot;
//        auto _key = target->bucket[loc].key;
//        if ((((hash_funcs[0](&target->bucket[loc].key, sizeof(Key_t), f_seed)
//                >> (8 * sizeof(s_hash) - target->local_depth)) != pattern) ||
//             (target->bucket[loc].key == INVALID)) && (target->bucket[loc].key != SENTINEL)) {
//            if (CAS(&target->bucket[loc].key, &_key, SENTINEL)) {
//                target->bucket[loc].value = value;
//                target->bucket[loc].key = key;
//                segment_write(dir->segmentPointers[x], target);
//                target->unlock();
//                segment_write(dir->segmentPointers[x], target);
//                delete target;
//                //cout << key << "插入成功2" << endl;
//                return;
//            }
//        }
//    }

    auto target_local_depth = target->local_depth;
    // COLLISION !!
    /* need to split segment but release the exclusive lock first to avoid deadlock */
    dir->unlock(segIndex);

    if (!dir->suspend(segIndex)) {
        std::this_thread::yield();
        goto RETRY;
    }

    /* need to check whether the target segment has been split */
    if (target_local_depth != target->local_depth) {
        dir->segLock[segIndex] = 0;
        std::this_thread::yield();
        goto RETRY;
    }

    auto newSegIndex = dir->segLock.size();
    //cout << key << "进入了split" << endl;
    target->Split(newSegIndex);

    DIR_RETRY:
    if (target->local_depth == dir->depth) {//dir扩容
        if (!dir->suspend()) {
            std::this_thread::yield();
            goto DIR_RETRY;
        }
        x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
        segIndex = dir->segIndex[x];
        dir->depth++;
        dir->capacity <<= 1;
        dir->segLock.push_back(0);
        auto old_segIndex = dir->segIndex;
        dir->segIndex.resize(dir->capacity);
        for (int i = 0; i < dir->capacity / 2; ++i) {
            if (i == x) {
                dir->segIndex[2 * i] = old_segIndex[i];
                dir->segIndex[2 * i + 1] = newSegIndex;
            } else {
                dir->segIndex[2 * i] = old_segIndex[i];
                dir->segIndex[2 * i + 1] = old_segIndex[i];
            }
        }
        dir->sema=0;
        target->local_depth++;
        local_depth_write(segIndex, reinterpret_cast<const char *>(&target->local_depth));
        dir->segLock[segIndex] = 0;
    } else {//dir不需要扩容
        while (!dir->lock()) {
            asm("nop");
        }
        x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
        segIndex = dir->segIndex[x];
        dir->segLock.push_back(0);//之前忘了加
        if (dir->depth == target->local_depth + 1) {
            if (x % 2 == 0) {
                dir->segIndex[x + 1] = newSegIndex;
            } else {
                dir->segIndex[x] = newSegIndex;
            }
            dir->unlock();
            target->local_depth++;
            local_depth_write(segIndex, reinterpret_cast<const char *>(&target->local_depth));
            dir->segLock[segIndex] = 0;
        } else {
            int stride = pow(2, dir->depth - target_local_depth);
            auto loc = x - (x % stride);
            for (int i = 0; i < stride / 2; ++i) {
                dir->segIndex[loc + stride / 2 + i] = newSegIndex;
            }
            dir->unlock();
            target->local_depth++;
            local_depth_write(segIndex, reinterpret_cast<const char *>(&target->local_depth));
            dir->segLock[segIndex] = 0;
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
        cout << "nop" << endl;
        asm("nop");
    }
    auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    auto segIndex = dir->segIndex[x];
//    if (key == 523346) {
//        cout << dec << key << "的segIndex:" << segIndex << endl;
//        cout << dec << key << "的x:" << x << endl;
//    }
    segment_read(segIndex, target);

    if (!dir->lock(segIndex)) {
        std::this_thread::yield();
        goto RETRY;
    }

    auto x_check = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    int segIndex_check = dir->segIndex[x_check];
    auto target_check = new struct Segment;
    segment_read(segIndex_check, target_check);
    if (!target->equal(target_check)) {
        dir->unlock(segIndex);
        delete target_check;
        std::this_thread::yield();
        goto RETRY;
    }
    delete target_check;

    for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (f_idx + i) % Segment::kNumSlot;
        if (target->bucket[loc].key == key) {
            Value_t v = target->bucket[loc].value;
            /* key found, release segment shared lock */
            dir->unlock(segIndex);
            delete target;
            return v;
        }
    }

//    auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
//    auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;
//    for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
//        auto loc = (s_idx + i) % Segment::kNumSlot;
//        if (target->bucket[loc].key == key) {
//            Value_t v = target->bucket[loc].value;
//            target->unlock();//ifdef INPLACE
//            delete target;
//            return v;
//        }
//    }
    /* key not found, release segment shared lock */
    dir->unlock(segIndex);
    //cout << key << "读取失败" << endl;
    return NONE;
}

void CCEH::Recovery() {

}

double CCEH::Utilization() {
    size_t sum = 0;
    size_t cnt = 0;
    auto target = new struct Segment;
    for (int i = 0; i < dir->capacity; ++cnt) {
        segment_read(dir->segIndex[i], target);
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
        segment_read(dir->segIndex[i], target);
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
            segment_read(dir->segIndex[i], target);
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

