// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

static void *
revokethread(void *x) {
    lock_server_cache *sc = (lock_server_cache *) x;
    sc->revoker();
    return 0;
}

static void *
retrythread(void *x) {
    lock_server_cache *sc = (lock_server_cache *) x;
    sc->retryer();
    return 0;
}

lock_server_cache::lock_server_cache(class rsm *_rsm) 
  : rsm (_rsm)
{
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  assert (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  assert (r == 0);
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t, int &) {
    lock_protocol::status ret = lock_protocol::OK;
    return ret;
}

rpcc *bindToClient(string dst) {
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    rpcc *cl = new rpcc(dstsock);
    // check return value here
    cl->bind();

    return cl;
}

lock_protocol::status
lock_server_cache::acquire(string cid, lock_protocol::lockid_t lid, int ver, int &) {
    std::cerr << "acquire: cid: " << cid << ". lid: " << lid << ". ver: " << ver << "\n";

    // check if client is already known, otherwise bind to client
    pthread_mutex_lock(&knownClientsLock);

    if (knownClients.find(cid) == knownClients.end()) {
        std::cerr << "Bind to previously unkown client: " << cid << "\n";
        knownClients[cid] = bindToClient(cid);
    }
    pthread_mutex_unlock(&knownClientsLock);


    pthread_mutex_lock(&infoLock);
    versionCounter[cid][lid] = ver;
    lock_protocol::status ret;

    lock_info info;

    // check if lock already exists
    if (infoMap.find(lid) != infoMap.end()) {
        info = infoMap[lid];
    } else {
        std::cerr << "Create new lock: " << lid << "\n";
        info.state = FREE;
    }

    // Lock is free: Set it to locked and return OK
    if (info.state == FREE) {
        info.state = LOCKED;
        info.owner = cid;
        ret = lock_protocol::OK;
        std::cerr << "Grant Lock: " << lid << " to client: " << cid << ". ver: " << ver << "\n";
    } else {
        // add current client to list of interested clients, tell client to retry
        info.interestedClients.push_back(cid);
        ret = lock_protocol::RETRY;
        std::cerr << "Cannot grant Lock: " << lid << " to client: " << cid << ". Lock already taken. ver: " << ver
                  << "\n";

        if (!info.revoking) {
            // tell current owner of lock to revoke it
            std::cerr << "Another client tries to acquire lock: " << lid << ". Sent revoke to current owner: "
                      << info.owner << "\n";
            pthread_mutex_lock(&revokeListLock);
            client_info i;
            i.lid = lid;
            i.cid = info.owner;
            i.ver = versionCounter[info.owner][lid];
            revokeList.push_back(i);
            pthread_cond_broadcast(&revokeCondition);
            pthread_mutex_unlock(&revokeListLock);
        }

        info.revoking = true;
    }
    infoMap[lid] = info;

    pthread_mutex_unlock(&infoLock);
    return ret;
}

lock_protocol::status
lock_server_cache::release(string cid, lock_protocol::lockid_t lid, int ver, int &) {
    std::cerr << "release: cid: " << cid << ". lid: " << lid << ". ver: " << ver << "\n";

    pthread_mutex_lock(&infoLock);

    versionCounter[cid][lid] = ver;
    lock_protocol::status ret = lock_protocol::OK;
    lock_info info;

    if (infoMap.find(lid) != infoMap.end()) {
        // get info
        info = infoMap[lid];

        // add client info to current list
        pthread_mutex_lock(&retryListLock);

        for (auto &cl : info.interestedClients) {
            client_info i;
            i.cid = cl;
            i.lid = lid;
            i.ver = versionCounter[cl][lid];
            retryList.push_back(i);
        }

        pthread_cond_broadcast(&retryCondition);
        pthread_mutex_unlock(&retryListLock);

        // empty list of interested clients
        info.interestedClients.clear();
    }

    if (cid == info.owner) {
        info.state = FREE;
        info.owner = "";
        info.revoking = false;
        infoMap[lid] = info;
        std::cerr << "Client: " << cid << " releases Lock: " << lid << "\n";
    }

    pthread_mutex_unlock(&infoLock);

    return ret;
}

void
lock_server_cache::revoker() {

    // This method should be a continuous loop, that sends revoke
    // messages to lock holders whenever another client wants the
    // same lock
    while (true) {
        pthread_mutex_lock(&revokeListLock);

        if (revokeList.empty()) {
            pthread_cond_wait(&revokeCondition, &revokeListLock);
        }

        if (!revokeList.empty()) {
            auto const &info = revokeList.back();
            revokeList.pop_back();
            pthread_mutex_unlock(&revokeListLock);

            int r;
            rpcc *client;
            pthread_mutex_lock(&knownClientsLock);
            client = knownClients[info.cid];
            pthread_mutex_unlock(&knownClientsLock);

            if (rsm->amiprimary()) {
                while (client->call(rlock_protocol::revoke, info.lid, info.ver, r) != rlock_protocol::OK) {
                    std::cerr << "[ERROR] Can't send revoke for lock: " << info.lid << " to client " << info.cid
                              << ". ver: " << info.ver << "\n";
                }
            }

            std::cerr << "Sent revoke for lock: " << info.lid << " to client " << info.cid << ". ver: " << info.ver
                      << "\n";
        }
    }
}


void
lock_server_cache::retryer() {

    // This method should be a continuous loop, waiting for locks
    // to be released and then sending retry messages to those who
    // are waiting for it.
    while (true) {
        pthread_mutex_lock(&retryListLock);

        if (retryList.empty()) {
            pthread_cond_wait(&retryCondition, &retryListLock);
        }

        if (!retryList.empty()) {
            auto const &info = retryList.back();
            retryList.pop_back();
            pthread_mutex_unlock(&retryListLock);

            int r;
            rpcc *client;
            pthread_mutex_lock(&knownClientsLock);
            client = knownClients[info.cid];
            pthread_mutex_unlock(&knownClientsLock);

            if (rsm->amiprimary()) {
                while (client->call(rlock_protocol::retry, info.lid, info.ver, r) != rlock_protocol::OK) {
                    std::cerr << "[ERROR] Can't Sent retry for lock: " << info.lid << " to client " << info.cid << ". ver: "
                              << info.ver << "\n";
                }
            }

            std::cerr << "Sent retry for lock: " << info.lid << " to client " << info.cid << ". ver: " << info.ver
                      << "\n";
        }
    }
}



