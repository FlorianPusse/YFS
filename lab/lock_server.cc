// the lock server implementation

#include <pthread.h>
#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h> 

lock_server::lock_server() :
        nacquire(0) {
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    printf("stat request from clt %d\n", clt);
    r = nacquire;
    return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r) {
    printf("acquire request from clt %d\n", clt);
    nacquire++;

    pthread_mutex_t *lock = nullptr;

    // Retrieve lock from map
    pthread_mutex_lock(&mapLock);
    if (lockMap.find(lid) != lockMap.end()) {
        lock = lockMap[lid];
    } else {
        lock = new pthread_mutex_t();
        pthread_mutex_init(lock, NULL);
        lockMap[lid] = lock;
    }
    pthread_mutex_unlock(&mapLock);

    // Lock the lock
    pthread_mutex_lock(lock);

    return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r) {
    printf("release request from clt %d\n", clt);

    pthread_mutex_t *lock = nullptr;

    // get the right lock
    pthread_mutex_lock(&mapLock);
    lock = lockMap.at(lid);
    pthread_mutex_unlock(&mapLock);

    // unlock the lock
    pthread_mutex_unlock(lock);

    return lock_protocol::OK;
}


