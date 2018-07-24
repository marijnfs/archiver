#include <blake2.h>
#include <glib-2.0/gio/gio.h>
#include <glib.h>
#include <iostream>
#include <lmdb.h>
#include <lz4.h>
#include <ctime>

#include <string>
#include <vector>
#include <tuple>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <capnp/serialize-packed.h>
//#include "serialise.h"
#include "files.capnp.h"
#include "bytes.h"

using namespace std;

string path = "./";
uint8_t key[BLAKE2B_KEYBYTES];
uint HASH_BYTES(32);
bool ONLY_ARCHIVE(true);

struct StringException : public std::exception {
  std::string str;
  StringException(std::string msg_) : str(msg_) {}

  const char *what() const noexcept { return str.c_str(); }
};

struct DB {
  DB() {
    cout << "opening" << endl;
    c(mdb_env_create(&env));
    c(mdb_env_set_mapsize(env, size_t(1) << 40)); // One TB
    c(mdb_env_open(env, "./testdb", MDB_NOSUBDIR, 0664));
    c(mdb_txn_begin(env, NULL, 0, &txn));
    c(mdb_dbi_open(txn, NULL, MDB_CREATE, &dbi));
    // char *bla = " ";
    // MDB_val mkey{1, bla}, mdata{1, bla};
    // c( mdb_put(txn, *dbi, &mkey, &mdata, 0) );
    c(mdb_txn_commit(txn));
    // cout << "done" << endl;
  }

  ~DB() { mdb_dbi_close(env, dbi); }

  //put function for vector types
  template <typename T, typename D>
  void put(std::vector<T> &key, vector<D> &data) {
    put(reinterpret_cast<uint8_t *>(&key[0]),
        reinterpret_cast<uint8_t *>(&data[0]), key.size() * sizeof(T),
        data.size() * sizeof(D));
  }

  //classic byte pointer put function
  void put(uint8_t *key, uint8_t *data, size_t key_len, size_t data_len) {
    MDB_val mkey{key_len, key}, mdata{data_len, data};

    c(mdb_txn_begin(env, NULL, 0, &txn));
    c(mdb_put(txn, dbi, &mkey, &mdata, MDB_NODUPDATA));
    c(mdb_txn_commit(txn));
  }

  Bytes *get(uint8_t *ptr, size_t len) {
    MDB_val mkey{len, ptr};
    MDB_val mdata;
    c(mdb_txn_begin(env, NULL, 0, &txn));
    int result = mdb_get(txn, dbi, &mkey, &mdata);
    if (result == MDB_NOTFOUND)
      return 0;
    auto ret_val = new Bytes(reinterpret_cast<uint8_t *>(mdata.mv_data),
                                            reinterpret_cast<uint8_t *>(mdata.mv_data) +
                                            mdata.mv_size);
    c(mdb_txn_commit(txn));
    return ret_val;
  }

  Bytes *get(std::vector<uint8_t> &key) {
    return get(&key[0], key.size());
  }

  bool has(std::vector<uint8_t> &key) {
    MDB_val mkey{key.size(), &key[0]};
    MDB_val mdata;
    c(mdb_txn_begin(env, NULL, 0, &txn));
    int result = mdb_get(txn, dbi, &mkey, &mdata);
    c(mdb_txn_commit(txn));
    return result != MDB_NOTFOUND;
  }

  void copy_db(string path) {
    c(mdb_env_copy2(env, path.c_str(), MDB_CP_COMPACT));
  }

  int rc;
  MDB_env *env = 0;
  MDB_dbi dbi;
  MDB_txn *txn = 0;

  //check function
  void c(int rc) {
    if (rc != 0) {
      fprintf(stderr, "txn->commit: (%d) %s\n", rc, mdb_strerror(rc));
      throw StringException("db error");
    }
  }
};

DB db;


uint64_t total_uncompressed(0), total_compressed(0);

Bytes get_hash(uint8_t *ptr, int len) {
    Bytes hash(HASH_BYTES);
    if (blake2b(&hash[0], ptr, key, HASH_BYTES, len, BLAKE2B_KEYBYTES) < 0)
      throw StringException("hash problem");
    return hash;
}
/*
struct RMessage {
  RMessage(Bytes &b) : flat_reader(b.kjwp()) {
  }
                                                
  template <typename T>
  T read() {
    return flat_reader.getRoot<T>(T());
  }

  ::capnp::FlatArrayMessageReader flat_reader;
};*/

struct Message {
  Message(){}
  capnp::MallocMessageBuilder msg;
  
  template <typename T>
  typename T::Builder build() {
    return msg.initRoot<T>();
  }

  //gives data pointer
  uint8_t *data() {
    serialise();
    return (uint8_t*) &wdata[0];
  }

  int size() {
    serialise();
    return wdata.asBytes().size();
  }

  void serialise() {
    if (!serialised)
      wdata = messageToFlatArray(msg);
    serialised = true;
  }

  Bytes hash() {
    return get_hash(data(), size());
  }

  bool serialised = false;
  kj::Array<capnp::word> wdata = 0;
};

tuple<Bytes, uint64_t> enumerate(GFile *root, GFile *file) {
  vector<uint8_t> dir_hash(HASH_BYTES);

  GFileEnumerator *enumerator;
  GError *error = NULL;

  enumerator = g_file_enumerate_children(
      file, "*", G_FILE_QUERY_INFO_NOFOLLOW_SYMLINKS, NULL, &error);

  if (error != NULL) {
    throw StringException(error->message);
  }

  vector<string> names;
  vector<uint64_t> sizes;
  vector<Bytes> hashes;
  vector<bool> is_dir;

  uint64_t total_size(0);

  while (TRUE) {
    GFile *child;
    GFileInfo *finfo;
    char *relative_path;
    char *base_name;

    if (!g_file_enumerator_iterate(enumerator, &finfo, &child, NULL, &error))
      break;
    if (!finfo)
      break;

    base_name = g_file_get_basename(child);
    relative_path = g_file_get_relative_path(root, child);
    auto file_type = g_file_info_get_file_type(finfo);

    //skip special files, like sockets
    if (file_type == G_FILE_TYPE_SPECIAL) {
      cout << "SKIPPING SPECIAL FILE: " << base_name << endl;
      continue;
    }

    if (string("testdb") == base_name) {
      cout << "SKIPPING DATABASE FILE: " << base_name << endl;
      continue;
    }

    //handle directories by recursively calling enumerate, which returns a hash and total size
    if (file_type == G_FILE_TYPE_DIRECTORY) {
      auto [hash, n] = enumerate(root, child); 
      names.push_back(base_name);
      hashes.push_back(hash);
      sizes.push_back(n);
      is_dir.push_back(true);
      total_size += n;
      continue;
    }

    
    //if we are here this should be a regular file, read it
    gchar *data = 0;
    gsize len(0);
    if (!g_file_get_contents(g_file_get_path(child), &data, &len, &error)) {
      cerr << "Read Error: " << g_file_get_path(child) << endl;
      continue;
    }
    
    //calculate its hash
    Bytes hash = get_hash((uint8_t*)data, len);

    for (auto h : hash)
      printf("%x", h);
    cout << endl;

    //compress the file and see how much you compress it
    const int max_compressed_len = LZ4_compressBound(len);
    // We will use that size for our destination boundary when allocating space.
    vector<char> compressed_data(max_compressed_len);
    const int compressed_data_len = LZ4_compress_default(
        data, &compressed_data[0], len, max_compressed_len);
    if (compressed_data_len < 0)
      throw StringException("compression failed");

    total_uncompressed += len;
    total_compressed += compressed_data_len;

    names.push_back(base_name);
    hashes.push_back(hash);
    sizes.push_back(len);
    is_dir.push_back(false);
    total_size += len;

    
    if (!ONLY_ARCHIVE)
      db.put(hash, compressed_data); //store compressed file in database
    g_free(data);
    // cout << g_file_info_get_name(finfo) << endl;

    cout << relative_path << " " << len << " " << compressed_data_len << " "
         << (static_cast<double>(total_uncompressed) / total_compressed)
         << endl;
    cout << total_uncompressed << " " << total_compressed << endl;
    g_assert(relative_path != NULL);
    g_free(relative_path);
    g_free(base_name);
  }
  g_file_enumerator_close(enumerator, NULL, &error);

  /// Dir serialiser
  Message dir_message;
  auto builder = dir_message.build<cap::Dir>();

  builder.setSize(total_size);
  int n_entries = names.size();

  auto entries_builder = builder.initEntries(n_entries);

  for (int n(0); n < n_entries; ++n) {
    auto b = entries_builder[n];
    b.setName(names[n]);
    b.setSize(sizes[n]);
    b.setHash(kj::arrayPtr(&hashes[n][0], hashes[n].size()));
    if (is_dir[n])
      b.setDir();
    else
      b.setFile();
  }  

  /// Get data and calculate the hash
  uint8_t *dir_data = dir_message.data();
  size_t data_len = dir_message.size();
  
  auto hash = dir_message.hash();
  
  if (blake2b(&hash[0], &dir_data[0], key, HASH_BYTES, data_len, BLAKE2B_KEYBYTES) < 0)
      throw StringException("hash problem");
  
  /// store it
  db.put(&hash[0], dir_data, hash.size(), data_len);

  return tuple<Bytes, uint64_t>(hash, total_size);
}

template <typename T>
std::ostream &operator<<(std::ostream &out, std::vector<T> &vec) {
  out << "[";
  if (vec.size())
    out << vec[0];
  for (int i(1); i < vec.size(); ++i)
    out << "," << vec[i];
  return out << "]";
}

void backup(GFile *path, string backup_name, string backup_description) {
  auto [dir_hash, backup_size] = enumerate(path, path);
  //we have root hash and size
  //save the root
  cout << dir_hash << " size:" << backup_size << endl;
  Message msg;
  auto b = msg.build<cap::Backup>();
  b.setName(backup_name);
  b.setDescription(backup_description);
  b.setHash(dir_hash.kjp());  
  b.setSize(backup_size);
  b.setTimestamp(std::time(0));

  auto backup_hash = msg.hash();

  db.put(&backup_hash[0], msg.data(), backup_hash.size(), msg.size());

  string rootstr("ROOT");
  db.put((uint8_t*)&rootstr[0], &backup_hash[0], rootstr.size(), backup_hash.size());
}

void read_backup() {
  string root_str("ROOT");
  auto root_hash = db.get((uint8_t*)&root_str[0], root_str.size());

  if (!root_hash)
    throw StringException("No root found");
  
  auto root = db.get(*root_hash);

  ::capnp::FlatArrayMessageReader flat_reader(root->kjwp());
  
  //RMessage reader(*data);
  auto r = flat_reader.getRoot<cap::Root>();
  auto backups = r.getBackups();
  cout << backups.size() << endl;
  
  for (auto b : backups) {
    Bytes bytes(b);
    db.get(bytes);
  }
}

struct Backup {
};

struct Archiver {
  DB db;

  //get all root names
  vector<string> get_backups() {
    return vector<string>();
  }

  Backup get_backup(string name) {
    return Backup();
  }

  //get root backup struct
  

  void get_children(Backup backup) {
  }
};

int main(int argc, char **argv) {
  //Set a standard key for the blake hash
  for (size_t i = 0; i < BLAKE2B_KEYBYTES; ++i)
    key[i] = (uint8_t)i;

  if (argc < 2) {
    cerr << "no command given, use: " << argv[0] << " [command] [options]" << endl;
    cerr << "command = [archive, dryrun, duplicate, filelist, list]" << endl;
    return -1;
  }
  
  string command(argv[1]);

  if (command == "archive") {
    if (argc != 4) {
      cerr << "usage: " << argv[0] << " " << command << " [name] [path]" << endl;
      return -1;
    }
    string name(argv[2]);
    
    ONLY_ARCHIVE = false;
    //get gfile to given path
    GFile *file = g_file_new_for_path(argv[3]);  
    
    //backup recursively  
    backup(file, name, "");
  }

  if (command == "dryrun") {
    if (argc != 4) {
      cerr << "usage: " << argv[0] << " " << command << " [name] [path]" << endl;
      return -1;
    }

    string name(argv[2]);
    
    ONLY_ARCHIVE = true;
    //get gfile to given path
    GFile *file = g_file_new_for_path(argv[3]);  
    
    //backup recursively  
    backup(file, name, "");
  }
  
  if (command == "duplicate") {
    if (argc != 3) {
      cerr << "usage: " << argv[0] << " " << command << " [path]" << endl;
      return -1;
    }
    
    db.copy_db(argv[2]);
  }

  if (command == "list") {
    if (argc != 2) {
      cerr << "usage: " << argv[0] << " " << command << endl;
      return -1;
    }
    
    
  }

  if (command == "filelist") {
    if (argc != 3) {
      cerr << "usage: " << argv[0] << " " << command << " [backup name]" << endl;
      return -1;
    }
    
    
  }
    

}
