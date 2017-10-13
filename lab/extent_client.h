// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "rpc.h"

class extent_client {
private:
    rpcc *cl;
    pthread_mutex_t mapLock;
    std::map <extent_protocol::extentid_t, std::string> files;
    std::map <extent_protocol::extentid_t, extent_protocol::attr> attributes;
    std::map<extent_protocol::extentid_t, bool> isDirty;
    std::map<extent_protocol::extentid_t, bool> toDelete;
public:
    extent_client(std::string dst);

    extent_protocol::status get(extent_protocol::extentid_t eid, std::string &buf);

    extent_protocol::status getattr(extent_protocol::extentid_t eid, extent_protocol::attr &a);

    extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);

    extent_protocol::status remove(extent_protocol::extentid_t eid);

    extent_protocol::status flush(extent_protocol::extentid_t eid);
};

#endif 

