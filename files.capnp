@0xaff13c93205b1f69;

struct Entry {
    name @0 : Text;
    size @1 : UInt64;
    hash @2 : Data;
    union {
        file @3 : Void;
        dir @4 : Void;
    }
}

struct Dir {
    entries @0 : List(Entry);
    size @1 : UInt64;
}

struct Root {
    name @0 : Text;
    description @1 : Text;
    size @2 : UIint64;
    hash @3 : Data;
}

struct Backup {
    description @0 : Text;
    root @1 : Data;
    timestamp @2 : UInt64;
}

struct Backups {
    roots @0 : List(Data);
    lastBackups @1 : Data;
}