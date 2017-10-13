#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>
#include <map>
#include "lock_client.h"
#include "lock_client_cache.h"

using namespace std;

class lock_release_user_implementation : public lock_release_user {
    extent_client *ec;

    void dorelease(lock_protocol::lockid_t);

public:
    lock_release_user_implementation(extent_client *ec) {
        this->ec = ec;
    }
};

class yfs_client {
    extent_client *ec;
    lock_client *lc;
public:

    typedef unsigned long long inum;
    enum xxstatus {
        OK, RPCERR, NOENT, IOERR, FBIG, EXISTING
    };
    typedef int status;

    struct fileinfo {
        unsigned long long size;
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirinfo {
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirent {
        std::string name;
        unsigned long long inum;
    };

private:
    static std::string filename(inum);

    static inum n2i(std::string);

    void lock(inum inum);

    void unlock(inum inum);

public:
    static std::string serializeDirectoryEntries(map <std::string, inum>);

    static map <std::string, inum> unserializeDirectoryEntries(std::string);

    yfs_client(std::string, std::string);

    bool isfile(inum);

    bool isdir(inum);

    inum ilookup(inum di, std::string name);

    int getfile(inum, fileinfo &);

    int getdir(inum, dirinfo &);

    // Our method
    int lookUp_ino(inum, std::string, inum &);

    int createNode(inum parent, std::string name, inum &newInum, bool isDirectory);

    int getDirData(inum dir, std::string &data);

    int setSize(inum, int);

    int write(inum, off_t off, std::string &data);

    int read(inum, size_t size, off_t off, std::string &data);

    int unlink(inum parent, std::string unlinkedItem);
};

#endif