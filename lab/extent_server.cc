// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <map>
#include <ctime>

extent_server::extent_server() {
    pthread_mutex_init(&mapLock, NULL);
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &) {
    pthread_mutex_lock(&mapLock);

    files[id] = buf;
    extent_protocol::attr newAttr;
    newAttr.size = buf.size();
    time_t currTime = std::time(nullptr);
    newAttr.atime = currTime;
    newAttr.mtime = currTime;
    newAttr.ctime = currTime;
    attributes[id] = newAttr;

    pthread_mutex_unlock(&mapLock);
    return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf) {
    pthread_mutex_lock(&mapLock);

    try {
        attributes.at(id).atime = std::time(nullptr);
        buf = files.at(id);

        pthread_mutex_unlock(&mapLock);
        return extent_protocol::OK;
    } catch (const std::out_of_range &oor) {
        pthread_mutex_unlock(&mapLock);
        return extent_protocol::NOENT;
    }
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a) {
    pthread_mutex_lock(&mapLock);

    try {
        a = attributes.at(id);

        pthread_mutex_unlock(&mapLock);
        return extent_protocol::OK;
    } catch (const std::out_of_range &oor) {
        pthread_mutex_unlock(&mapLock);
        return extent_protocol::NOENT;
    }
}

int extent_server::remove(extent_protocol::extentid_t id, int &) {
    pthread_mutex_lock(&mapLock);

    auto file_it = files.find(id);
    if (file_it != files.end()) {
        files.erase(file_it);
    }
    auto attr_it = attributes.find(id);
    if (attr_it != attributes.end()) {
        attributes.erase(attr_it);
    }

    pthread_mutex_unlock(&mapLock);
    return extent_protocol::OK;
}

