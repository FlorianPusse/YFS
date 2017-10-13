// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <ctime>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst) {
    pthread_mutex_init(&mapLock, NULL);

    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    if (cl->bind() != 0) {
        printf("extent_client: bind failed\n");
    }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf) {
    std::cerr << "GET called, eid: " << eid << "\n";

    pthread_mutex_lock(&mapLock);
    extent_protocol::status ret = extent_protocol::OK;

    if (files.find(eid) != files.end() && (!toDelete[eid])) {
        std::cerr << "GET called, eid: " << eid << ". Load cached value\n";
        buf = files[eid];
    } else if (files.find(eid) != files.end() && (toDelete[eid])) {
        ret = extent_protocol::NOENT;
    } else {
        std::cerr << "GET called, eid: " << eid << ". Retrieve value from server\n";
        // CALL SERVER, ASK FOR DATA
        ret = cl->call(extent_protocol::get, eid, buf);
        files[eid] = buf;

        extent_protocol::attr &attr = attributes[eid];
        ret = cl->call(extent_protocol::getattr, eid, attr);

        // new file, can't be dirty yet
        isDirty[eid] = false;
        toDelete[eid] = false;
    }

    attributes[eid].atime = std::time(nullptr);

    std::cerr << "GET called, eid: " << eid << ". Return value: " << buf << "\n";

    pthread_mutex_unlock(&mapLock);
    return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
                       extent_protocol::attr &attr) {
    std::cerr << "GETATTR called, eid: " << eid << "\n";

    pthread_mutex_lock(&mapLock);
    extent_protocol::status ret = extent_protocol::OK;

    if (attributes.find(eid) != attributes.end() && (!toDelete[eid])) {
        attr = attributes.at(eid);
    } else if (attributes.find(eid) != attributes.end() && (toDelete[eid])) {
        ret = extent_protocol::NOENT;
    } else {
        // CALL SERVER, ASK FOR ATTRIBUTES
        extent_protocol::attr &a = attributes[eid];
        ret = cl->call(extent_protocol::getattr, eid, a);
        attr = a;
    }

    pthread_mutex_unlock(&mapLock);
    return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf) {
    std::cerr << "PUT called, eid: " << eid << "Store value: " << buf << "\n";

    pthread_mutex_lock(&mapLock);
    extent_protocol::status ret = extent_protocol::OK;

    // set data
    files[eid] = buf;

    // create and set attributes
    extent_protocol::attr newAttr;
    newAttr.size = buf.size();
    time_t currTime = std::time(nullptr);
    newAttr.atime = currTime;
    newAttr.mtime = currTime;
    newAttr.ctime = currTime;
    attributes[eid] = newAttr;

    // mark data as dirty
    isDirty[eid] = true;
    toDelete[eid] = false;

    pthread_mutex_unlock(&mapLock);
    return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid) {
    std::cerr << "REMOVE called, eid: " << eid << "\n";

    pthread_mutex_lock(&mapLock);
    extent_protocol::status ret = extent_protocol::OK;

    // file should be marked as deleted
    toDelete[eid] = true;

    pthread_mutex_unlock(&mapLock);
    return ret;
}

extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid) {
    pthread_mutex_lock(&mapLock);
    std::cerr << "FLUSH called, eid: " << eid << "\n";

    extent_protocol::status ret = extent_protocol::OK;
    int r;

    if (files.find(eid) != files.end()) {
        if (toDelete[eid]) {
            std::cerr << "Propagate delete of extent " << eid << " to extent server\n";
            while (cl->call(extent_protocol::remove, eid, r) != extent_protocol::OK);
        } else if (isDirty[eid]) {
            std::cerr << "Propagate update of extent " << eid << " to extent server. Value: " << files[eid] << "\n";
            while (cl->call(extent_protocol::put, eid, files[eid], r) != extent_protocol::OK);
        }

        files.erase(eid);
        attributes.erase(eid);
        toDelete.erase(eid);
        isDirty.erase(eid);
    }

    pthread_mutex_unlock(&mapLock);
    return ret;
}


