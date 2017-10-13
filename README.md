# YFS - Yet-Another File System

YFS is a distributed file system in the spirit of Frangipani. The base verision is part of the Distributed Systems course at Saarland University.

## Idea

Clients run an instance of the YFS client on their machine. All files are stored as so called "extents" on remote extent servers. Lock servers make sure that conflicts between multiple YFS clients are taken care of. To increase fault tolerance (due to network errors or server crashes), Paxos is applied.

## Technical details

tba.

## Build:

Tested on Ubuntu 16.4

- Install FUSE (sudo apt install libfuse2 libfuse-dev)
- Install g++ (Tested with version 6.3)
- make
