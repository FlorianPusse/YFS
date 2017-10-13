#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include "rsm.h"
#include "vector"

class lock_server_cache {
private:
    class rsm *rsm;
    enum lock_state {
        FREE, LOCKED
    };
    struct lock_info {
        lock_state state;
        string owner;
        vector <string> interestedClients;
        bool revoking = false;
    };
    struct client_info {
        string cid;
        lock_protocol::lockid_t lid;
        int ver;
    };
    map<string, rpcc *> knownClients;
    map <lock_protocol::lockid_t, lock_info> infoMap;
    map <string, map<lock_protocol::lockid_t, int>> versionCounter;
    vector <client_info> revokeList;
    vector <client_info> retryList;
    pthread_mutex_t infoLock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t revokeListLock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t retryListLock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t knownClientsLock = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t retryCondition = PTHREAD_COND_INITIALIZER;
    pthread_cond_t revokeCondition = PTHREAD_COND_INITIALIZER;
public:
    lock_server_cache(class rsm *rsm = 0);

    lock_protocol::status stat(lock_protocol::lockid_t, int &);

    lock_protocol::status acquire(string id, lock_protocol::lockid_t lid, int ver, int &);

    lock_protocol::status release(string id, lock_protocol::lockid_t lid, int ver, int &);

    void revoker();

    void retryer();
};

#endif
