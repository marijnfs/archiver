@0xaff13c93205b1f69;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("cap");

struct Entry {
    name @0 : Text; #file/dir name
    size @1 : UInt64; #byte size of data (uncompressed)
    hash @2 : Data; #hash of uncompressed file
    union {
        file @3 : Void;
        dir @4 : Void;
    }
}

struct Dir {
    entries @0 : List(Entry);
    size @1 : UInt64; #byte size of data
}

struct Backup {
    name @0 : Text;
    description @1 : Text; #option for description
    size @2 : UInt64;  #size of total original file size
    hash @3 : Data;
    timestamp @4 : UInt64;
}

#root backup struct to store pointers to current backups
struct Root {
    backups @0 : List(Data);
    lastRoot @1 : Data; #hash of last backups object
}