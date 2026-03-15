// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef pactreeAPI_H
#define pactreeAPI_H
#include <bits/stdint-uintn.h>
#include "pactreeImpl.h"
#include "common.h"
class pactree{
public:
    pactreeImpl *pt;
    pactree(int numa) {
        pt = initPT(numa);
        //hl = new HydraListImpl(numa);
    }
    ~pactree() {
        //delete hl;
    }
    bool insert(Key_t key, Val_t val) {
        return pt->insert(key, val);
    }
    bool update(Key_t key, Val_t val) {
        return pt->update(key, val);
    }
    Val_t lookup(Key_t key) {
        return pt->lookup(key);
    }
    Val_t remove(Key_t key) {
        return pt->remove(key);
    }
    uint64_t scan(Key_t startKey, int range, std::vector<Val_t> &result) {
        return pt->scan(startKey, range, result);
    }
    uint64_t scanThenUnlockAll(Key_t startKey, int range, std::vector<Val_t> &result) {
        return pt->scanThenUnlockAll(startKey, range, result);
    }

    uint64_t rangeQuery(Key_t startKey, Key_t endKey, std::vector<Val_t> &result) {
        return pt->rangeQuery(startKey, endKey, result);
    }

    uint64_t rangeQueryThenUnlockAll(Key_t startKey, Key_t endKey, std::vector<Val_t> &result) {
        return pt->rangeQueryThenUnlockAll(startKey, endKey, result);
    }

    void registerThread() {
        pt->registerThread();
    }
    void unregisterThread() {
        pt->unregisterThread();
    }
};

#endif 
