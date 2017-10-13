// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "lock_client.h"
#include <thread>
#include <chrono>
#include "lock_client_cache.h"


void
lock_release_user_implementation::dorelease(lock_protocol::lockid_t lid) {
    int ret;
    while ((ret = ec->flush(lid)) != extent_protocol::OK);
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client(extent_dst);
    lock_release_user_implementation *impl = new lock_release_user_implementation(ec);
    lc = new lock_client_cache(lock_dst, impl);

    srand(getpid());

    // make root available at startup
    inum root = 1;
    extent_protocol::attr rootAttr;

    lock(root);
    if (ec->getattr(root, rootAttr) == extent_protocol::NOENT) {
        if (ec->put(root, string("")) != extent_protocol::OK) {
            printf("cannot create root!\n");
        }
    }
    unlock(root);
}

yfs_client::inum
yfs_client::n2i(std::string n) {
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

void yfs_client::lock(inum inum) {
    while (lc->acquire(inum) != lock_protocol::OK) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void yfs_client::unlock(inum inum) {
    while (lc->release(inum) != lock_protocol::OK) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

std::string
yfs_client::filename(inum inum) {
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum) {
    return (inum & 0x80000000);
}

bool
yfs_client::isdir(inum inum) {
    return !isfile(inum);
}

// http://stackoverflow.com/questions/14265581/parse-split-a-string-in-c-using-string-delimiter-standard-c
vector <string>
split(string s, string delimiter) {
    size_t pos = 0;
    vector <string> output;
    string token;

    while ((pos = s.find(delimiter)) != std::string::npos) {
        token = s.substr(0, pos);
        output.push_back(token);
        s.erase(0, pos + delimiter.length());
    }

    output.push_back(s);
    return output;
}

std::string
yfs_client::serializeDirectoryEntries(std::map <std::string, yfs_client::inum> directory) {
    string result = "";

    for (auto const &iterator : directory) {
        string name = iterator.first;
        inum inum = iterator.second;
        result += name + string(";") + filename(inum) + string("\n");
    }

    return result;
}

std::map <std::string, yfs_client::inum>
yfs_client::unserializeDirectoryEntries(std::string serializedDirectory) {
    map <std::string, yfs_client::inum> directories;

    for (const string &row : split(serializedDirectory, "\n")) {
        if (!row.empty()) {
            vector <string> entry = split(row, ";");
            string name = entry.at(0);
            inum inum = n2i(entry.at(1));
            directories[name] = inum;
        }
    }

    return directories;
}

int
yfs_client::getfile(inum inum, fileinfo &fin) {
    int ret = IOERR;
    lock(inum);

    printf("getfile %016llx\n", inum);

    extent_protocol::attr a;

    if (ec->getattr(inum, a) == extent_protocol::OK) {
        fin.atime = a.atime;
        fin.mtime = a.mtime;
        fin.ctime = a.ctime;
        fin.size = a.size;
        printf("getfile %016llx -> sz %llu\n", inum, fin.size);

        ret = OK;
    }

    unlock(inum);
    return ret;
}

int
yfs_client::getdir(inum inum, dirinfo &din) {
    int ret = IOERR;

    lock(inum);

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) == extent_protocol::OK) {
        din.atime = a.atime;
        din.mtime = a.mtime;
        din.ctime = a.ctime;

        ret = OK;
    }

    unlock(inum);
    return ret;
}

int
yfs_client::unlink(inum parent, std::string unlinkedItem) {
    int ret = NOENT;
    inum fileInum;

    lock(parent);

    std::string data;

    if ((ret = ec->get(parent, data)) == extent_protocol::OK) {
        auto dirData = unserializeDirectoryEntries(data);

        auto e = dirData.find(unlinkedItem);

        if (e != dirData.end() && !isdir(e->second)) {
            fileInum = e->second;
            dirData.erase(e);

            ret = ec->put(parent, serializeDirectoryEntries(dirData));

            if (ret == extent_protocol::OK) {
                lock(fileInum);
                ec->remove(fileInum);
                unlock(fileInum);
            }
        }
    }

    unlock(parent);
    return ret;
}

int
yfs_client::createNode(inum parent, std::string name, inum &newInum, bool isDirectory) {
    lock(parent);
    int ret;

    std::string serializedDirectory;

    if (!isdir(parent)) {
        ret = NOENT;
    } else if ((ret = ec->get(parent, serializedDirectory)) == extent_protocol::OK) {
        auto directory = unserializeDirectoryEntries(serializedDirectory);

        if (directory.find(name) == directory.end()) {
            // file doesn't exist yet. everything is ok.
            // continue by creating a new file
            // create new inum first.

            newInum = std::rand();

            if (isDirectory) {
                newInum &= 0x7FFFFFFF;
            } else {
                newInum |= 0x80000000;
            }

            lock(newInum);

            // create the file itself
            if (ec->put(newInum, string("")) != extent_protocol::OK) {
                ret = IOERR;
            } else {
                // add "file" to directory and update it
                directory[name] = newInum;

                // update directory
                if (ec->put(parent, serializeDirectoryEntries(directory)) != extent_protocol::OK) {
                    ret = IOERR;
                } else {
                    ret = OK;
                }
            }
            unlock(newInum);
        } else {
            if (isDirectory) {
                ret = EXISTING;
            } else {
                newInum = directory[name];
                ret = OK;
            }
        }
    }

    unlock(parent);
    return ret;
}

int
yfs_client::lookUp_ino(inum parent, std::string name, inum &i) {
    lock(parent);
    int ret = NOENT;

    // store directory data in string
    std::string data;

    if (ec->get(parent, data) == extent_protocol::OK) {
        auto directory = unserializeDirectoryEntries(data);
        auto e = directory.find(name);

        if (e != directory.end()) {
            i = e->second;
            ret = OK;
        }
    }

    unlock(parent);
    return ret;
}

int
yfs_client::getDirData(inum dir, std::string &data) {
    lock(dir);
    auto ret = ec->get(dir, data);
    unlock(dir);

    return ret;
}

int yfs_client::setSize(inum ino, int size) {
    int ret = NOENT;
    lock(ino);
    std::string buf;

    if (ec->get(ino, buf) == extent_protocol::OK) {
        buf.resize(size);
        ec->put(ino, buf);

        while (lc->release(ino) != lock_protocol::OK);
        ret = OK;
    }

    unlock(ino);
    return ret;
}

int yfs_client::write(inum ino, off_t off, string &data) {
    int ret = IOERR;

    lock(ino);
    std::string buf;

    if (ec->get(ino, buf) == extent_protocol::OK) {
        if (((size_t) off) > buf.size()) {
            buf.resize(off);
            buf.append(data);
        } else {
            buf.replace(off, data.size(), data);
        }

        ec->put(ino, buf);

        ret = OK;
    }

    unlock(ino);
    return ret;
}

int yfs_client::read(inum ino, size_t size, off_t off, string &data) {
    int ret = IOERR;

    lock(ino);
    std::string buf;

    if (ec->get(ino, buf) == extent_protocol::OK) {
        if (off < 0) {
            data = buf.substr(0, size);
        } else if (off + size > buf.size()) {
            data = buf.substr(off);
            data.resize(size, '\0');
        } else {
            data = buf.substr(off, size);
        }

        ret = OK;
    }

    unlock(ino);
    return ret;
}
