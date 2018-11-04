#ifndef __DB_H__
#define __DB_H__

#include <lmdb.h>
#include <vector>
#include <iostream>

#include "bytes.h"

char *DBNAME = "archiver.db";


enum Overwrite {
  OVERWRITE = 0,
  NOOVERWRITE = 1
};

struct DB {
  DB() {
    std::cerr << "opening" << std::endl;
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
  bool put(std::vector<T> &key, std::vector<D> &data, Overwrite overwrite = OVERWRITE) {
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

  void copy_db(std::string path) {
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

#endif
