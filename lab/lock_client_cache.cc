// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>

static void *
releasethread(void *x) {
    lock_client_cache *cc = (lock_client_cache *) x;
    cc->releaser();
    return 0;
}

static void *
revoker_loop_thread(void *x) {
    lock_client_cache *cc = (lock_client_cache *) x;
    cc->revoker_loop();
    return 0;
}

static void *
retryer_loop_thread(void *x) {
    lock_client_cache *cc = (lock_client_cache *) x;
    cc->retryer_loop();
    return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
                                     class lock_release_user *_lu)
        : lock_client(xdst), lu(_lu) {
    srand(time(NULL) ^ last_port);
    rlock_port = ((rand() % 32000) | (0x1 << 10));
    const char *hname;
    // assert(gethostname(hname, 100) == 0);
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlock_port;
    id = host.str();
    last_port = rlock_port;
    rpcs *rlsrpc = new rpcs(rlock_port);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);
    rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);

    rsmc = new rsm_client(xdst);

    /* register RPC handlers with rlsrpc */
    pthread_t th;
    int r = pthread_create(&th, NULL, &releasethread, (void *) this);
    assert(r == 0);
    r = pthread_create(&th, NULL, &revoker_loop_thread, (void *) this);
    assert(r == 0);
    r = pthread_create(&th, NULL, &retryer_loop_thread, (void *) this);
    assert(r == 0);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid) {
    std::cerr << id << ": Acquire called for lock: " << lid << " \n";

    // check the lock's state
    pthread_mutex_lock(&infoLock);

    if (infoMap.find(lid) == infoMap.end()) {
        lock_info i;
        i.state = NONE;
        lockMap[lid] = PTHREAD_MUTEX_INITIALIZER;
        releaseConditionMap[lid] = PTHREAD_COND_INITIALIZER;
        unlockedConditionMap[lid] = PTHREAD_COND_INITIALIZER;
        retryConditionMap[lid] = PTHREAD_COND_INITIALIZER;
        requestDoneConditionMap[lid] = PTHREAD_COND_INITIALIZER;
        versionCounter[lid] = 0;
        infoMap[lid] = i;
    }

    lock_info &info = infoMap[lid];
    pthread_mutex_unlock(&infoLock);

    std::cerr << id << ": Acquire called for lock: " << lid << ". Got info\n";

    // get the lock itself and try to acquire it.
    pthread_mutex_lock(&lockMap[lid]);

    std::cerr << id << ": Acquire called for lock: " << lid << ". Got lock\n";

    while (info.state != FREE) {
        // depending on the locks state, just give it away, or try to aquire it
        if (info.state == NONE) {
            // try to acquire lock
            info.state = ACQUIRING;
            int r;
            pthread_mutex_unlock(&lockMap[lid]);
            lock_protocol::status ret = rsmc->call(lock_protocol::acquire, id, lid, versionCounter[lid], r);
            pthread_mutex_lock(&lockMap[lid]);
            std::cerr << id << ": Try to acquire lock: " << lid << " Sent version: " << versionCounter[lid] << "\n";
            versionCounter[lid]++;

            if (ret == lock_protocol::OK) {
                std::cerr << id << ": Got Lock: " << lid << ". Increase version " << (versionCounter[lid] - 1) << " -> "
                          << versionCounter[lid] << "\n";
                break;
            } else if (ret == lock_protocol::RETRY) {
                // if we get a retry message, continue next loop iteration and try again
                std::cerr << id << ": Got RETRY for Lock: " << lid << ". Increase version " << (versionCounter[lid] - 1)
                          << " -> " << versionCounter[lid] << "\n";
                assert(info.state = ACQUIRING);
                pthread_cond_wait(&retryConditionMap[lid], &lockMap[lid]);
                info.state = NONE;
                continue;
            } else {
                info.state = NONE;
                pthread_mutex_unlock(&lockMap[lid]);
                return lock_protocol::RPCERR;
            }
        } else if (info.state == ACQUIRING) {
            // wait until lock is acquired and freed
            pthread_cond_wait(&unlockedConditionMap[lid], &lockMap[lid]);
        } else if (info.state == LOCKED) {
            // wait for lock to be unlocked
            pthread_cond_wait(&unlockedConditionMap[lid], &lockMap[lid]);
        } else if (info.state == RELEASING) {
            // wait for lock to be released. try to acquire afterwards
            pthread_cond_wait(&releaseConditionMap[lid], &lockMap[lid]);
        } else {
            assert(false);
        }
    }

    std::cerr << id << ": Acquired Lock: " << lid << ". Version: " << versionCounter[lid] << "\n";

    // if we manage to get here, the lock is available now
    info.state = LOCKED;
    info.owner = pthread_self();

    pthread_mutex_unlock(&lockMap[lid]);
    return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid) {
    std::cerr << id << ",  Release lock: " << lid << " \n";

    // get info und free lock
    pthread_mutex_lock(&infoLock);
    lock_info &info = infoMap[lid];
    pthread_mutex_unlock(&infoLock);
    bool lockAvailable = false;

    pthread_mutex_lock(&lockMap[lid]);

    if (!info.toRevoke) {
        info.state = FREE;
        lockAvailable = true;
    } else {
        // need to release lock at server
        info.state = RELEASING;
        pthread_mutex_lock(&toRevokeLocksLock);
        toRevokeLocks.push(lid);
        // toRevokeLocks.insert(toRevokeLocks.begin(), lid);
        pthread_cond_broadcast(&releaserCondition);
        pthread_mutex_unlock(&toRevokeLocksLock);
    }

    // signal everyone that lock is now unlocked (if we didn't release at server)
    if (lockAvailable) {
        pthread_cond_broadcast(&unlockedConditionMap[lid]);
    }

    pthread_mutex_unlock(&lockMap[lid]);

    return lock_protocol::OK;
}

void
lock_client_cache::releaser() {
    // This method should be a continuous loop, waiting to be notified of
    // freed locks that have been revoked by the server, so that it can
    // send a release RPC.
    while (true) {
        pthread_mutex_lock(&toRevokeLocksLock);

        if (toRevokeLocks.empty()) {
            pthread_cond_wait(&releaserCondition, &toRevokeLocksLock);
        }

        if (!toRevokeLocks.empty()) {
            auto &lid = toRevokeLocks.front();
            toRevokeLocks.pop();
            // toRevokeLocks.pop_back();
            pthread_mutex_unlock(&toRevokeLocksLock);

            // get the info object from map
            pthread_mutex_lock(&infoLock);
            lock_info &info = infoMap[lid];
            pthread_mutex_unlock(&infoLock);

            pthread_mutex_lock(&lockMap[lid]);
            int version = versionCounter[lid];
            if (info.state != RELEASING) {
                pthread_mutex_unlock(&lockMap[lid]);
                continue;
            }
            pthread_mutex_unlock(&lockMap[lid]);

            std::cerr << id << ",: Try to send lock back to server: " << lid << ".Sent Version: " << versionCounter[lid]
                      << "\n";
            lu->dorelease(lid);
            int r;
            while (rsmc->call(lock_protocol::release, id, lid, version, r) != lock_protocol::OK);
            std::cerr << id << ": Send lock back to server: " << lid << ".Sent Version: " << versionCounter[lid]
                      << "\n";

            pthread_mutex_lock(&lockMap[lid]);
            // set its state back to NONE.
            info.state = NONE;
            info.toRevoke = false;
            pthread_cond_broadcast(&releaseConditionMap[lid]);
            pthread_mutex_unlock(&lockMap[lid]);

            continue;
        }

        pthread_mutex_unlock(&toRevokeLocksLock);
    }
}

rlock_protocol::status
lock_client_cache::retry(lock_protocol::lockid_t lid, int ver, int &) {
    // tell threads they can try to aquire.
    processing_info info;
    info.lid = lid;
    info.ver = ver;

    pthread_mutex_lock(&retryListLock);
    retryList.push(info);
    // retryList.insert(retryList.begin(), info);
    pthread_cond_broadcast(&retryerCondition);
    pthread_mutex_unlock(&retryListLock);

    return rlock_protocol::OK;
}

void
lock_client_cache::retryer_loop() {
    while (true) {
        pthread_mutex_lock(&retryListLock);

        if (retryList.empty()) {
            pthread_cond_wait(&retryerCondition, &retryListLock);
        }

        if (!retryList.empty()) {
            processing_info i = retryList.front();
            retryList.pop();
            // retryList.pop_back();

            pthread_mutex_lock(&lockMap[i.lid]);

            if (i.ver < versionCounter[i.lid]) {
                pthread_cond_broadcast(&retryConditionMap[i.lid]);
            } else {
                retryList.push(i);
                // retryList.insert(retryList.begin(), i);
            }

            std::cerr << id << ": Got retry for Lock: " << i.lid << ". Versions: " << i.ver << "/"
                      << versionCounter[i.lid] << "\n";

            pthread_mutex_unlock(&lockMap[i.lid]);
        }

        pthread_mutex_unlock(&retryListLock);
    }
}

rlock_protocol::status
lock_client_cache::revoke(lock_protocol::lockid_t lid, int ver, int &) {
    // set to revoke
    processing_info info;
    info.lid = lid;
    info.ver = ver;

    pthread_mutex_lock(&revokeListLock);
    revokeList.push(info);
    // revokeList.insert(revokeList.begin(), info);
    pthread_cond_broadcast(&revokerCondition);
    pthread_mutex_unlock(&revokeListLock);

    return rlock_protocol::OK;
}

void
lock_client_cache::revoker_loop() {
    while (true) {
        pthread_mutex_lock(&revokeListLock);

        if (revokeList.empty()) {
            pthread_cond_wait(&revokerCondition, &revokeListLock);
        }

        if (!revokeList.empty()) {
            processing_info i = revokeList.front();
            revokeList.pop();
            // revokeList.pop_back();
            pthread_mutex_lock(&lockMap[i.lid]);

            if (i.ver < versionCounter[i.lid]) {
                pthread_mutex_lock(&infoLock);
                lock_info &info = infoMap[i.lid];
                pthread_mutex_unlock(&infoLock);

                if (info.state == FREE) {
                    info.state = RELEASING;
                    pthread_mutex_lock(&toRevokeLocksLock);
                    toRevokeLocks.push(i.lid);
                    // toRevokeLocks.insert(toRevokeLocks.begin(), i.lid);
                    pthread_cond_broadcast(&releaserCondition);
                    pthread_mutex_unlock(&toRevokeLocksLock);
                } else {
                    info.toRevoke = true;
                }

                std::cerr << id << ": Got revoke request for Lock: " << i.lid
                          << ". Set toRevoke to true. Wait for release. Lock state: " << (info.state) << ". Versions: "
                          << i.ver << "/" << versionCounter[i.lid] << "\n";
            } else {
                revokeList.push(i);
                std::cerr << id << ": Can't revoke Lock: " << i.lid << " yet: " << i.ver << ", "
                          << versionCounter[i.lid] << ".Lock state: " << (infoMap[i.lid].state) << "\n";
            }

            pthread_mutex_unlock(&lockMap[i.lid]);
        }

        pthread_mutex_unlock(&revokeListLock);
    }
}

