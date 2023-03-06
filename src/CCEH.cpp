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

using namespace std;


void local_depth_read(int64_t index, char *target) {
    if (index < 0) LOG_INFO("a%d", index);
    StoreMng::GetInstance()->Read(index * sizeof(Segment) + kSegmentSize, sizeof(size_t), target);
}

void local_depth_write(int64_t index, const char *target) {
    if (index < 0) LOG_INFO("b%d", index);
    StoreMng::GetInstance()->Write(index * sizeof(Segment) + kSegmentSize, sizeof(size_t), target);
}

void bucket_write(int64_t index, int loc, const char *target) {
    if (index < 0) LOG_INFO("c%d", index);
    StoreMng::GetInstance()->Write(index * sizeof(Segment) + loc * sizeof(Pair), sizeof(Pair), target);
}

void segment_read(int64_t index, Segment *target) {
    if (index < 0) LOG_INFO("d%d", index);
    StoreMng::GetInstance()->Read(index * sizeof(Segment), sizeof(Segment), (char *) target);
}

void segment_write(int64_t index, Segment *target) {
    if (index < 0) LOG_INFO("e%d", index);
    StoreMng::GetInstance()->Write(index * sizeof(Segment), sizeof(Segment), (char *) target);
}


bool Segment::Insert4split(Key_t &key, Value_t value, size_t loc) {
    for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (loc + i) % kNumSlot;
        if (bucket[slot].key == INVALID) {
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

    auto pattern = ((size_t) 1 << (sizeof(Key_t) * 8 - local_depth - 1));
    for (int i = 0; i < kNumSlot; ++i) {
        auto f_hash = hash_funcs[0](&bucket[i].key, sizeof(Key_t), f_seed);
        if (f_hash & pattern) {
            //split->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask) * kNumPairPerCacheLine);
            if (!split->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask) * kNumPairPerCacheLine)) {
                auto s_hash = hash_funcs[2](&bucket[i].key, sizeof(Key_t), s_seed);
                if (!split->Insert4split(bucket[i].key, bucket[i].value, (s_hash & kMask) * kNumPairPerCacheLine)) {

                }
            }
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
    if (!dir->lock()) {
        //LOG_INFO("key:%d, value:%d segLock:%ddir上锁失败", key, value, dir->sema);
        std::this_thread::yield();
        goto RETRY;
    }
    auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    int segIndex = dir->segIndex[x];
    if (segIndex < 0) LOG_INFO("d%d", segIndex);
    if (!dir->lock(segIndex)) {
        dir->unlock();
        //LOG_INFO("key:%d, value:%d segIndex:%d seglock:%d 上锁失败", key, value, segIndex, dir->segLock[segIndex]);
        std::this_thread::yield();
        goto RETRY;
    }

    segment_read(segIndex, target);

    auto target_check = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    int segIndex_check = dir->segIndex[target_check];
    auto target_check_seg = new struct Segment;
    if (segIndex < 0) LOG_INFO("d%d", segIndex);
    segment_read(segIndex_check, target_check_seg);
    if (!target->equal(target_check_seg)) {
        dir->unlock(segIndex);
        dir->unlock();
        delete target_check_seg;
        std::this_thread::yield();
        goto RETRY;
    }
    delete target_check_seg;
    dir->unlock(segIndex);
    dir->unlock();

    if (!dir->suspend(segIndex)) {
        //LOG_INFO("key:%d, value:%d segIndex:%d segLock:%d 上写锁失败", key, value, segIndex, dir->segLock[segIndex]);
        std::this_thread::yield();
        goto RETRY;
    }
    auto pattern = (f_hash >> (8 * sizeof(f_hash) - target->local_depth));
    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (f_idx + i) % Segment::kNumSlot;
        auto _key = target->bucket[loc].key;
        if ((((hash_funcs[0](&target->bucket[loc].key, sizeof(Key_t), f_seed)
                >> (8 * sizeof(f_hash) - target->local_depth)) != pattern) ||
             (target->bucket[loc].key == INVALID)) && (target->bucket[loc].key != SENTINEL)) {
            if (CAS(&target->bucket[loc].key, &_key, SENTINEL)) {
                target->bucket[loc].value = value;
                //mfence();
                target->bucket[loc].key = key;
                bucket_write(segIndex, loc, reinterpret_cast<const char *>(&target->bucket[loc]));
                delete target;
                dir->segLock[segIndex] = 0;
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
                //mfence();
                target->bucket[loc].key = key;
                bucket_write(segIndex, loc, reinterpret_cast<const char *>(&target->bucket[loc]));
                delete target;
                dir->segLock[segIndex] = 0;
                return;
            }
        }
    }

    auto target_local_depth = target->local_depth;
    // COLLISION !!
//    /* need to split segment but release the exclusive lock first to avoid deadlock */
    dir->segLock[segIndex] = 0;
//
//    if (!dir->suspend(segIndex)) {
//        LOG_INFO("key:%d, value:%d segIndex:%d 上写锁失败", key, value, segIndex);
//        std::this_thread::yield();
//        goto RETRY;
//    }

    /* need to check whether the target segment has been split */
//    if (target_local_depth != target->local_depth) {
//        dir->segLock[segIndex] = 0;
//        std::this_thread::yield();
//        goto RETRY;
//    }
    DIR_RETRY:
    if (!dir->suspend(segIndex)) {
        //LOG_INFO("key:%d, value:%d segIndex:%d segLock:%d 上写锁失败", key, value, segIndex, dir->segLock[segIndex]);
        std::this_thread::yield();
        goto DIR_RETRY;
    }
    if (!dir->suspend()) {
        //LOG_INFO("key:%d, value:%d segIndex:%d segLock:%d dir上写锁失败", key, value, segIndex, dir->sema);
        dir->segLock[segIndex] = 0;
        std::this_thread::yield();
        goto DIR_RETRY;
    }

    auto newSegIndex = dir->segLock.size();
    //LOG_INFO("key: %d value: %d OldSegIndex:%d NewSegindex:%ld", key, value, segIndex, newSegIndex);
    target->Split(newSegIndex);

    if (target->local_depth == dir->depth) {//dir扩容
//        if (!dir->suspend()) {
//            LOG_INFO("key:%d, value:%d segIndex:%d dir上写锁失败", key, value, segIndex);
//            std::this_thread::yield();
//            goto DIR_RETRY;
//        }
//        x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
//        segIndex = dir->segIndex[x];
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
        dir->sema = 0;
        target->local_depth++;
        local_depth_write(segIndex, reinterpret_cast<const char *>(&target->local_depth));
        dir->segLock[segIndex] = 0;
    } else {//dir不需要扩容
//        while (!dir->lock()) {
//            LOG_INFO("key:%d, value:%d segIndex:%d dir上锁失败", key, value, segIndex);
//            asm("nop");
//        }
//        x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
//        segIndex = dir->segIndex[x];
        dir->segLock.push_back(0);
        if (dir->depth == target->local_depth + 1) {
            if (x % 2 == 0) {
                dir->segIndex[x + 1] = newSegIndex;
            } else {
                dir->segIndex[x] = newSegIndex;
            }
            dir->sema = 0;
            target->local_depth++;
            local_depth_write(segIndex, reinterpret_cast<const char *>(&target->local_depth));
            dir->segLock[segIndex] = 0;
        } else {
            int stride = pow(2, dir->depth - target_local_depth);
            auto loc = x - (x % stride);
            for (int i = 0; i < stride / 2; ++i) {
                dir->segIndex[loc + stride / 2 + i] = newSegIndex;
            }
            dir->sema = 0;
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
        asm("nop");
    }
    auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    auto segIndex = dir->segIndex[x];
    if (segIndex < 0) LOG_INFO("d%d", segIndex);
    segment_read(segIndex, target);

    if (!dir->lock(segIndex)) {
        std::this_thread::yield();
        goto RETRY;
    }

    auto x_check = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    int segIndex_check = dir->segIndex[x_check];
    auto target_check = new struct Segment;
    if (segIndex < 0) LOG_INFO("d%d", segIndex);
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

    auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
    auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;
    for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (s_idx + i) % Segment::kNumSlot;
        if (target->bucket[loc].key == key) {
            Value_t v = target->bucket[loc].value;
            dir->unlock(segIndex);//ifdef INPLACE
            delete target;
            return v;
        }
    }
    /* key not found, release segment shared lock */
    dir->unlock(segIndex);
    return NONE;
}

void CCEH::Recovery() {

}

//double CCEH::Utilization() {
//    size_t sum = 0;
//    size_t cnt = 0;
//    auto target = new struct Segment;
//    for (int i = 0; i < dir->capacity; ++cnt) {
//        segment_read(dir->segIndex[i], target);
//        int stride = pow(2, dir->depth - target->local_depth);
//        auto pattern = (i >> (dir->depth - target->local_depth));
//        for (unsigned j = 0; j < Segment::kNumSlot; ++j) {
//            auto f_hash = h(&target->bucket[j].key, sizeof(Key_t));
//            if (((f_hash >> (8 * sizeof(f_hash) - target->local_depth)) == pattern) &&
//                (target->bucket[j].key != INVALID)) {
//                sum++;
//            }
//        }
//        i += stride;
//    }
//    delete target;
//    return ((double) sum) / ((double) cnt * Segment::kNumSlot) * 100.0;
//}
//
//size_t CCEH::Capacity(void) {
//    size_t cnt = 0;
//    auto target = new struct Segment;
//    for (int i = 0; i < dir->capacity; cnt++) {
//        segment_read(dir->segIndex[i], target);
//        int stride = pow(2, dir->depth - target->local_depth);
//        i += stride;
//    }
//    delete target;
//    return cnt * Segment::kNumSlot;
//}

// for debugging
//Value_t CCEH::FindAnyway(Key_t &key) {
//    auto target = new struct Segment;
//    for (size_t i = 0; i < dir->capacity; ++i) {
//        for (size_t j = 0; j < Segment::kNumSlot; ++j) {
//            segment_read(dir->segIndex[i], target);
//            if (target->bucket[j].key == key) {
//                cout << "segment(" << i << ")" << endl;
//                cout << "global_depth(" << dir->depth << "), local_depth(" << target->local_depth << ")" << endl;
//                cout << "pattern: " << bitset<sizeof(int64_t)>(i >> (dir->depth - target->local_depth)) << endl;
//                cout << "Key MSB: "
//                     << bitset<sizeof(int64_t)>(h(&key, sizeof(key)) >> (8 * sizeof(key) - target->local_depth))
//                     << endl;
//                return target->bucket[j].value;
//            }
//        }
//    }
//    delete target;
//    return NONE;
//}

