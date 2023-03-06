#ifndef CCEH_H_
#define CCEH_H_

#include <cstring>
#include <vector>
#include <cmath>
#include <vector>
#include <unordered_map>
#include <cstdlib>
#include <pthread.h>
#include <iostream>
#include <fstream>
#include "util.h"
#include "logging.h"
#include "singleton.h"
#include "lock.h"
#include <mutex>

const int MAX_FILES = 1000;
const int SEG_PER_FILE = 256;

typedef size_t Key_t;
typedef const char *Value_t;
typedef uint64_t Pointer;

const Key_t SENTINEL = -2;
const Key_t INVALID = -1;
const Value_t NONE = 0x0;

struct Pair {
    Key_t key;
    Value_t value;
};

class CCEH;

struct Directory;
struct Segment;

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits) - 1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 8;
constexpr size_t kCuckooThreshold = 16;

//constexpr size_t kCuckooThreshold = 32;
struct Segment {
    static const size_t kNumSlot = kSegmentSize / sizeof(Pair);

    Segment(void) {}

    ~Segment(void) {}

    bool equal(Segment *a) {
        if (local_depth != a->local_depth) return false;
        for (int i = 0; i < kNumSlot; i++) {
            if (bucket[i].key != a->bucket[i].key || bucket[i].value != a->bucket[i].value) return false;
        }
        return true;
    }

    void initSegment(void) {
        for (int i = 0; i < kNumSlot; ++i) {
            bucket[i].key = INVALID;
        }
        local_depth = 0;
    }

    void initSegment(size_t depth) {
        for (int i = 0; i < kNumSlot; ++i) {
            bucket[i].key = INVALID;
        }
        local_depth = depth;
    }

    int Insert(Key_t &, Value_t, size_t, size_t);

    bool Insert4split(Key_t &, Value_t, size_t);

    void Split(size_t);

    std::vector<std::pair<size_t, size_t>> find_path(size_t, size_t);

    void execute_path(std::vector<std::pair<size_t, size_t>> &, Key_t &, Value_t);

    void execute_path(std::vector<std::pair<size_t, size_t>> &, Pair);

    size_t numElement(void);

    Pair bucket[kNumSlot];
    //int64_t sema = 0;
    size_t local_depth;

};

const int MAX_FILE_SIZE = SEG_PER_FILE * sizeof(Segment);

struct StoreMng : public Singleton<StoreMng> {
    void Write(int64_t off, int len, const char *buf) {
        auto &writer = FStream(off);
        file_lock[off / MAX_FILE_SIZE].lock();
        writer.seekp(off % MAX_FILE_SIZE, std::ios::beg);
        writer.write(buf, len);
        file_lock[off / MAX_FILE_SIZE].unlock();
    }

    void Read(int64_t off, int len, char *buf) {
        auto &reader = FStream(off);
        file_lock[off / MAX_FILE_SIZE].lock();
        reader.seekg(off % MAX_FILE_SIZE, std::ios::beg);
        reader.read(buf, len);
        file_lock[off / MAX_FILE_SIZE].unlock();
    }

private:
    std::fstream &FStream(int64_t off) {
        LOG_ASSERT(off >= 0, "%ld", off);
        int idx = off / MAX_FILE_SIZE;
        map_lock.lock();
        if (openFiles.find(idx) == openFiles.end()) {
            std::string filename = "data/chunk" + std::to_string(idx) + ".dat";
            segFile[idx].open(filename, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
            openFiles[idx] = true;
        }
        map_lock.unlock();
        return segFile[idx];
    }

    std::fstream segFile[MAX_FILES];
    std::mutex file_lock[MAX_FILES];
    std::unordered_map<int, bool> openFiles;
    SpinMutex map_lock;
};


struct Directory {
    static const size_t kDefaultDepth = 10;
    int64_t sema = 0;
    size_t capacity;
    size_t depth;
    std::vector<size_t> segIndex;
    std::vector<int64_t> segLock;

    bool suspend(void) {
        int64_t val;
        do {
            val = sema;
            if (val < 0)
                return false;
        } while (!CAS(&sema, &val, -1));

        int64_t wait = 0 - val - 1;
        while (val && sema != wait) {
            asm("nop");
        }
        return true;
    }

    bool lock(void) {
        int64_t val = sema;
        while (val > -1) {
            if (CAS(&sema, &val, val + 1))
                return true;
            val = sema;
        }
        return false;
    }

    void unlock(void) {
        int64_t val = sema;
        while (!CAS(&sema, &val, val - 1)) {
            val = sema;
        }
    }

    //segment锁
    bool suspend(size_t i) {
        int64_t val;
        do {
            val = segLock[i];
            if (val < 0)
                return false;
        } while (!CAS(&segLock[i], &val, -1));

        int64_t wait = 0 - val - 1;
        while (val && segLock[i] != wait) {
            asm("nop");
        }
        return true;
    }

    bool lock(int i) {
        int64_t val = segLock[i];
        while (val > -1) {
            if (CAS(&segLock[i], &val, val + 1))
                return true;
            val = segLock[i];
        }
        return false;
    }

    void unlock(int i) {
        int64_t val = segLock[i];
        while (!CAS(&segLock[i], &val, val - 1)) {
            val = segLock[i];
        }
    }

    Directory(void) {}

    ~Directory(void) {}

    void initDirectory(void) {
        segLock.reserve(SEG_PER_FILE * MAX_FILES);//没加的时候会在push_back时，重新分配空间，多线程会危险（其实还是不是很懂
        depth = kDefaultDepth;
        capacity = pow(2, depth);
        sema = 0;
        for (int i = 0; i < capacity; ++i) {
            segIndex.push_back(i);
            segLock.push_back(0);
        }
    }

    void initDirectory(size_t _depth) {
        segLock.reserve(SEG_PER_FILE * MAX_FILES);
        depth = _depth;
        capacity = pow(2, _depth);
        sema = 0;
        for (int i = 0; i < capacity; ++i) {
            segIndex.push_back(i);
            segLock.push_back(0);
        }
    }
};

class CCEH {
public:
    CCEH(void) {}

    ~CCEH(void) {}

    void initCCEH();

    void initCCEH(size_t);

    void Insert(Key_t &, Value_t);

    bool InsertOnly(Key_t &, Value_t);

    bool Delete(Key_t &);

    Value_t Get(Key_t &);

    Value_t FindAnyway(Key_t &);

    double Utilization(void);

    size_t Capacity(void);

    void Recovery();

    bool crashed = true;
private:
    struct Directory *dir;
};

#endif
