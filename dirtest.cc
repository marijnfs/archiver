#include <blake2.h>
#include <glib-2.0/gio/gio.h>
#include <glib.h>
#include <iostream>
#include <lmdb.h>
#include <lz4.h>

#include <string>
#include <vector>
#include <tuple>
#include <capnp/serialize.h>
//#include "serialise.h"
#include "files.capnp.h"


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

  template <typename T, typename D>
  void put(std::vector<T> &key, vector<D> &data) {
    put(reinterpret_cast<uint8_t *>(&key[0]),
        reinterpret_cast<uint8_t *>(&data[0]), key.size() * sizeof(T),
        data.size() * sizeof(D));
  }

  void put(uint8_t *key, uint8_t *data, size_t key_len, size_t data_len) {
    MDB_val mkey{key_len, key}, mdata{data_len, data};

    c(mdb_txn_begin(env, NULL, 0, &txn));
    c(mdb_put(txn, dbi, &mkey, &mdata, MDB_NODUPDATA));
    c(mdb_txn_commit(txn));
  }

  std::vector<uint8_t> get(std::vector<uint8_t> &key) {
    MDB_val mkey{key.size(), &key[0]};
    MDB_val mdata;
    c(mdb_txn_begin(env, NULL, 0, &txn));
    c(mdb_get(txn, dbi, &mkey, &mdata));
    std::vector<uint8_t> ret_val(reinterpret_cast<uint8_t *>(mdata.mv_data),
                                 reinterpret_cast<uint8_t *>(mdata.mv_data) +
                                     mdata.mv_size);
    c(mdb_txn_commit(txn));
    return ret_val;
  }

  int rc;
  MDB_env *env = 0;
  MDB_dbi dbi;
  MDB_txn *txn = 0;

  void c(int rc) {
    if (rc != 0) {
      fprintf(stderr, "txn->commit: (%d) %s\n", rc, mdb_strerror(rc));
      throw StringException("db error");
    }
  }
};

DB db;


uint64_t total_uncompressed(0), total_compressed(0);

tuple<vector<uint8_t>, uint64_t> enumerate(GFile *root, GFile *file) {
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
  vector<vector<uint8_t>> hashes;
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

    if (file_type == G_FILE_TYPE_SPECIAL) {
      cout << "SKIPPING SPECIAL FILE: " << base_name << endl;
      continue;
    }

    if (string("testdb") == base_name) {
      cout << "SKIPPING DATABASE FILE: " << base_name << endl;
      continue;
    }

    if (file_type == G_FILE_TYPE_DIRECTORY) {
      auto [hash, n] = enumerate(root, child); 
      names.push_back(base_name);
      hashes.push_back(hash);
      sizes.push_back(n);
      is_dir.push_back(true);
      total_size += n;
      continue;
    }

    gchar *data = 0;
    gsize len(0);
    if (!g_file_get_contents(g_file_get_path(child), &data, &len, &error)) {
      cerr << "Read Error: " << g_file_get_path(child) << endl;
      continue;
    }

    vector<uint8_t> hash(HASH_BYTES);

    if (blake2b(&hash[0], data, key, HASH_BYTES, len, BLAKE2B_KEYBYTES) < 0)
      throw StringException("hash problem");

    for (auto h : hash)
      printf("%x", h);
    cout << endl;

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
      db.put(hash, compressed_data);
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
  capnp::MallocMessageBuilder dir_message;
  auto builder = dir_message.initRoot<Dir>();

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
  auto dir_data = messageToFlatArray(dir_message);
  size_t data_len = dir_data.asBytes().size();
  
  vector<uint8_t> hash(HASH_BYTES);
  
  if (blake2b(&hash[0], &dir_data[0], key, HASH_BYTES, data_len, BLAKE2B_KEYBYTES) < 0)
      throw StringException("hash problem");
  
  /// store it
  db.put(&hash[0], (uint8_t*)&dir_data[0], hash.size(), data_len);

  return tuple<vector<uint8_t>, uint64_t>(hash, total_size);
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

void backup(GFile *root, string root_description, string backup_description) {
  auto [root_hash, root_size] = enumerate(root, root);
  
  cout << root_hash << " size:" << root_size << endl;
  
}

int main(int argc, char **argv) {
  if (argc != 2) {
    cerr << "no path given" << endl;
    return -1;
  }

  for (size_t i = 0; i < BLAKE2B_KEYBYTES; ++i)
    key[i] = (uint8_t)i;

  GFile *file = g_file_new_for_path(argv[1]);
  backup(file, "", "");
}
