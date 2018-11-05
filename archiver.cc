#include <blake2.h>
#include <glib-2.0/gio/gio.h>
#include <glib.h>
#include <iostream>
#include <lmdb.h>
#include <lz4.h>
#include <ctime>

#include <functional>
#include <fstream>
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
char *DBNAME = "archiver.db";
uint8_t key[BLAKE2B_KEYBYTES];
uint HASH_BYTES(32);
bool ONLY_ARCHIVE(true);

uint64_t MAX_FILESIZE = uint64_t(0) * 1024 * 1024;
uint64_t MULTIPART_SIZE(uint64_t(2) << 30);

struct StringException : public std::exception {
  std::string str;
  StringException(std::string msg_) : str(msg_) {}

  const char *what() const noexcept { return str.c_str(); }
};

enum Overwrite {
  OVERWRITE = 0,
  NOOVERWRITE = 1
};

struct DB {
  DB() {
    cerr << "opening database under " << DBNAME << endl;
    c(mdb_env_create(&env));
    c(mdb_env_set_mapsize(env, size_t(1) << 40)); // One TB
    //c(mdb_env_open(env, DBNAME, MDB_NOSUBDIR, 0664));
    c(mdb_env_open(env, DBNAME, MDB_NOSUBDIR | MDB_WRITEMAP | MDB_MAPASYNC, 0664));
    
    c(mdb_txn_begin(env, NULL, 0, &txn));
    c(mdb_dbi_open(txn, NULL, MDB_CREATE, &dbi));
    // char *bla = " ";
    // MDB_val mkey{1, bla}, mdata{1, bla};
    // c( mdb_put(txn, *dbi, &mkey, &mdata, 0) );
    c(mdb_txn_commit(txn));
    // cout << "done" << endl;
  }

  ~DB() { 
    mdb_env_sync(env, 1);
    mdb_dbi_close(env, dbi); 
  }

  //put function for vector types
  template <typename T, typename D>
  bool put(std::vector<T> &key, vector<D> &data, Overwrite overwrite = OVERWRITE) {
    return put(reinterpret_cast<uint8_t *>(&key[0]),
        reinterpret_cast<uint8_t *>(&data[0]), key.size() * sizeof(T),
        data.size() * sizeof(D), overwrite);
  }

  //classic byte pointer put function
  bool put(uint8_t *key, uint8_t *data, size_t key_len, size_t data_len, Overwrite overwrite = OVERWRITE) {
    MDB_val mkey{key_len, key}, mdata{data_len, data};

    c(mdb_txn_begin(env, NULL, 0, &txn));
    int result = mdb_put(txn, dbi, &mkey, &mdata, (overwrite == NOOVERWRITE) ? MDB_NOOVERWRITE : 0);
    c(mdb_txn_commit(txn));
    if (result == MDB_KEYEXIST)
      return false;

    c(result);
    return true;
  }

  Bytes *get(uint8_t *ptr, size_t len) {
    MDB_val mkey{len, ptr};
    MDB_val mdata;
    c(mdb_txn_begin(env, NULL, 0, &txn));
    int result = mdb_get(txn, dbi, &mkey, &mdata);
    c(mdb_txn_commit(txn));
    if (result == MDB_NOTFOUND)
      return 0;
    auto ret_val = new Bytes(reinterpret_cast<uint8_t *>(mdata.mv_data),
                             reinterpret_cast<uint8_t *>(mdata.mv_data) +
                                            mdata.mv_size);
    
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
  vector<bool> is_multi;

  uint64_t total_size(0);
  static ofstream logfile("log.txt");

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
      cerr << "SKIPPING SPECIAL FILE: " << base_name << endl;
      continue;
    }

    if (string("archiver.db") == base_name) {
      cerr << "SKIPPING DATABASE FILE: " << base_name << endl;
      continue;
    }

    //handle directories by recursively calling enumerate, which returns a hash and total size
    if (file_type == G_FILE_TYPE_DIRECTORY) {
      auto [hash, n] = enumerate(root, child); 
      names.push_back(base_name);
      hashes.push_back(hash);
      sizes.push_back(n);
      is_dir.push_back(true);
      is_multi.push_back(false);
      total_size += n;
      continue;
    }

    goffset filesize = g_file_info_get_size(finfo);
    cerr << relative_path << " " << filesize << endl;

    if (MAX_FILESIZE && filesize > MAX_FILESIZE) { //on first pass ignore huge files

      logfile << "skipping: " << relative_path << " " << filesize << endl;
      continue;
    }

    if (filesize > MULTIPART_SIZE) { //Too big for direct storage, Multipart file
      cout << "writing multipart" << endl;
      vector<uint8_t> data(MULTIPART_SIZE);
      auto input_stream = g_file_read (child, NULL, &error);
      if (error != NULL) {
        throw StringException(error->message);
      }

      vector<Bytes> hashes;
      gsize len(0);
      while (true) {
        gsize bytes_read = g_input_stream_read (G_INPUT_STREAM(input_stream),
                       (void*)&data[0],
                       MULTIPART_SIZE,
                       NULL,
                       &error);
        if (error != NULL) {
          throw StringException(error->message);
        }
        if (bytes_read == 0)
          break;
        len += bytes_read;

        Bytes hash = get_hash((uint8_t*)&data[0], bytes_read);
        hashes.push_back(hash);

        if (!ONLY_ARCHIVE) {
          cout << "putting multipart: " << hash << " len: " << bytes_read << endl;
          db.put(hash.ptr(), (uint8_t*)&data[0], hash.size(), bytes_read, NOOVERWRITE); //store compressed file in database
        }

      }

      Message multipart_msg;
      auto mb = multipart_msg.build<cap::MultiPart>();
      auto pb = mb.initParts(hashes.size());
      for (int i(0); i < hashes.size(); ++i) {
        pb.set(i, hashes[i].kjp());
      }
      auto multipart_hash = multipart_msg.hash();
      if (!ONLY_ARCHIVE) {
        cout << "putting multipart object: " << multipart_hash << endl;
        db.put(multipart_hash.ptr(), multipart_msg.data(), multipart_hash.size(), multipart_msg.size(), NOOVERWRITE); //store compressed file in database
      }

      names.push_back(base_name);
      hashes.push_back(multipart_hash);
      sizes.push_back(len);
      is_dir.push_back(false);
      is_multi.push_back(true);
      total_size += len;

      g_free(input_stream);
    } else {
      //calculate its hash
            
      //if we are here this should be a regular file, read it
      gchar *data = 0;
      gsize len(0);
      if (!g_file_get_contents(g_file_get_path(child), &data, &len, &error)) {
        cerr << "Read Error: " << g_file_get_path(child) << endl;
        continue;
      }
      cerr << "len: " << len << endl;
      
      Bytes hash = get_hash((uint8_t*)data, len);

      cerr << hash << endl;

      /*
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
      */

      names.push_back(base_name);
      hashes.push_back(hash);
      sizes.push_back(len);
      is_dir.push_back(false);
      is_multi.push_back(false);
      total_size += len;
      
    
      if (!ONLY_ARCHIVE)
        db.put(hash.ptr(), (uint8_t*)data, hash.size(), len, NOOVERWRITE); //store compressed file in database
      //db.put(hash, compressed_data, NOOVERWRITE); //store compressed file in database
      g_free(data);
      // cout << g_file_info_get_name(finfo) << endl;
    }
    
    cerr << "total:" << total_size << endl;
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
    else if (is_multi[n])
      b.setMulti();
    else
      b.setFile();
  }  

  /// Get data and calculate the hash
  uint8_t *dir_data = dir_message.data();
  size_t data_len = dir_message.size();
  
  auto hash = dir_message.hash();
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


struct Backup {
  string name;
  string description;

  uint64_t size = 0;
  Bytes hash;
  uint64_t timestamp = 0;

};

Bytes *get_root_hash() {
  string root_str("ROOT");
  auto root_hash = db.get((uint8_t*)&root_str[0], root_str.size());
  return root_hash;
}

vector<Backup> get_backups(Bytes root_hash) {
  vector<Backup> returns;
  cerr << root_hash << endl;
  auto root = db.get(root_hash);
  if (!root)
    throw StringException("root not found");
  
  ::capnp::FlatArrayMessageReader flat_reader(*root);
  auto r = flat_reader.getRoot<cap::Root>();

  auto backups = r.getBackups();
  cerr << backups.size() << endl;
  
  for (auto b : backups) {
    Backup backup;
    Bytes backup_hash(b);
    auto backup_data = db.get(backup_hash);
    
    ::capnp::FlatArrayMessageReader flat_reader(*backup_data);
    auto r = flat_reader.getRoot<cap::Backup>();
    backup.name = r.getName();
    backup.description = r.getDescription();
    backup.size = r.getSize();
    backup.hash = Bytes(r.getHash());
    backup.timestamp = r.getTimestamp();
    returns.push_back(backup);
  }
  return returns;
}

Bytes *get_file(Bytes hash, int len) {
  auto data = db.get(hash);
  if (!data)
    StringException("file not found");
  return data;
  //auto output = new Bytes(len);
  //len = LZ4_decompress_safe (data->ptr<const char*>(), output->ptr<char *>(), data->size(), len);
  //return output;
}

void print_entry(cap::Entry::Reader& entry, string &full_name) {
  cerr << "f " << full_name << " " << entry.getSize() << " " << entry.getHash() << endl;
}

void recurse(Bytes hash, std::function<void(cap::Entry::Reader&, string&)> func = print_entry, string dir_name = "/") {
  auto dir_data = db.get(hash);
  ::capnp::FlatArrayMessageReader flat_reader(*dir_data);
  auto r = flat_reader.getRoot<cap::Dir>();
  //cout << "d " << dir_name << " " << r.getSize() << endl;
  for (auto entry : r.getEntries()) {
    string full_name = dir_name + string(entry.getName());
    func(entry, full_name);
    if (entry.isDir())
      recurse(entry.getHash(), func, dir_name + string(entry.getName()) + "/");
  }
}

void backup(GFile *path, string backup_name, string backup_description) {
  auto [dir_hash, backup_size] = enumerate(path, path);
  //we have root hash and size
  //save the root
  cerr << "root dir hash: " << dir_hash << " size:" << backup_size << endl;

  // Get current root if there is one
  auto root_hash = get_root_hash();
  if (root_hash) {
    auto backups = get_backups(*root_hash);
    bool replaced(false);
    int n(0);
    for (auto &b : backups) {
      if (b.name == backup_name) {
        cerr << "found backup name, replacing" << endl;
        backups.erase(backups.begin() + n);
        break;
      }
      ++n;
    }
    if (!replaced) {
      Backup b;
      b.name = backup_name;
      b.description = backup_description;
      b.size = backup_size;
      b.timestamp = std::time(0);
      b.hash = dir_hash;
      backups.push_back(b);
    }
    

    Message root_msg;
    auto root_b = root_msg.build<cap::Root>();
    root_b.setTimestamp(std::time(0));
    auto backups_build = root_b.initBackups(backups.size());
    n = 0;
    for (auto &backup : backups) {
        Message msg;
        auto b = msg.build<cap::Backup>();
        b.setName(backup.name);
        b.setDescription(backup.description);
        b.setHash(backup.hash.kjp());
        b.setSize(backup.size);
        b.setTimestamp(backup.timestamp);
        
        auto backup_hash = msg.hash();
        db.put(&backup_hash[0], msg.data(), backup_hash.size(), msg.size());
        backups_build.set(n, backup_hash.kjp());
        ++n;
    }
    
    root_b.setLastRoot(root_hash->kjp());
    *root_hash = root_msg.hash();
    db.put(root_hash->ptr(), root_msg.data(), root_hash->size(), root_msg.size());
    
  } else {
    cerr << "no current root found" << endl;
    Message msg;
    auto b = msg.build<cap::Backup>();
    b.setName(backup_name);
    b.setDescription(backup_description);
    b.setHash(dir_hash.kjp());
    b.setSize(backup_size);
    b.setTimestamp(std::time(0));
    
    auto backup_hash = msg.hash();
    cerr << "hash: " << backup_hash << " " << msg.size() << endl;
    msg.data();
    msg.size();
    db.put(&backup_hash[0], msg.data(), backup_hash.size(), msg.size());
        
    Message root_msg;
    auto root_b = root_msg.build<cap::Root>();
    root_b.setTimestamp(std::time(0));
    auto backups_build = root_b.initBackups(1);
    backups_build.set(0, backup_hash.kjp());
    
    root_hash = new Bytes(root_msg.hash());
    db.put(root_hash->ptr(), root_msg.data(), root_hash->size(), root_msg.size());
  }

  cerr << "storing root: " << *root_hash << endl;
  string rootstr("ROOT");
  db.put((uint8_t*)&rootstr[0], root_hash->ptr(), rootstr.size(), root_hash->size());
}

string timestring(uint64_t timestamp) {
 std::tm * ptm = std::localtime((time_t*)&timestamp);
 char buffer[32];
 // Format: Mo, 15.06.2009 20:20:00
 std::strftime(buffer, 32, "%a, %d.%m.%Y %H:%M:%S", ptm); 
 return string(buffer, 32);
}

void list_backups() {
  auto root_hash = get_root_hash();
  if (!root_hash)
    throw StringException("No root");
  auto backups = get_backups(*root_hash);
  for (auto &b : backups)
    cerr << b.name << " " << b.size << " " << timestring(b.timestamp) << endl;
}

void list_files(string backup_name) {
  auto root_hash = get_root_hash();
  if (!root_hash)
    throw StringException("No root");
  auto backups = get_backups(*root_hash);
  for (auto &b : backups) {
    cerr << b.name << " " << backup_name << endl;
    if (b.name == backup_name) {
      cerr << "found " << b.name << " with hash " << b.hash << endl;
      recurse(b.hash);
    }
  }
}

void output_file(string backup_name, string full_name) {
  auto f = [full_name](cap::Entry::Reader& entry, string &full_name_){
    if (entry.isFile()) {
      //cout << full_name_ << endl;
      if (full_name == full_name_) {
        auto data = get_file(entry.getHash(), entry.getSize());
        for (auto b : *data)
          cout << b;
      } 
    }
  };  

  auto root_hash = get_root_hash();
  if (!root_hash)
    throw StringException("No root");
  auto backups = get_backups(*root_hash);
  for (auto &b : backups) {
    cerr << b.name << " " << backup_name << endl;
    if (b.name == backup_name) {
      cerr << "found " << b.name << " with hash " << b.hash << endl;
      recurse(b.hash, f);
    }
  }

}

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
    
    list_backups();
  }

  if (command == "filelist") {
    if (argc != 3) {
      cerr << "usage: " << argv[0] << " " << command << " [backup name]" << endl;
      return -1;
    }
    
    list_files(argv[2]);
  }
    
  if (command == "output") {
    if (argc != 4) {
      cerr << "usage: " << argv[0] << " " << command << " [backup name] [file name]" << endl;
      return -1;
    }
    output_file(argv[2], argv[3]);
  }
}
