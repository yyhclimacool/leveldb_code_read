#include <glog/logging.h>

#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"

int main(int argc, char** argv) {
  gflags::SetVersionString("v0.0.1");
  // google::SetCommandLineOption("flagfile", "conf/gflags.conf");
  // google::SetCommandLineOption("log_dir", "./log/");
  // google::SetCommandLineOption("logbuflevel", "google::GLOG_WARNING");
  // google::SetCommandLineOption("logbufsecs", "0");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // google::InitGoogleLogging(argv[0]);
  // google::FlushLogFiles(google::GLOG_INFO);
  LOG(INFO) << "Hello leveldb";

  // Open db
  leveldb::DB* db;
  leveldb::Options op;
  op.create_if_missing = true;
  op.compression = leveldb::kSnappyCompression;
  op.block_cache = leveldb::NewLRUCache(10 * 1024 * 1024);
  std::string db_name = "./data/testdb";
  auto status = leveldb::DB::Open(op, db_name, &db);
  if (status.ok()) {
    LOG(INFO) << "create db success, name[" << db_name << "]";
  } else {
    LOG(ERROR) << "create db failed, name[" << db_name << "], status["
               << status.ToString() << "]";
    return -1;
  }

  // get a non-exist key
  leveldb::Slice key("key1");
  std::string value;
  status = db->Get(leveldb::ReadOptions(), key, &value);
  if (status.ok()) {
    LOG(INFO) << "get key[" << key.ToString() << "] success, value[" << value
              << "]";
  } else {
    LOG(ERROR) << "get key[" << key.ToString() << "] failed, status["
               << status.ToString() << "]";
  }

  // put a key
  status = db->Put(leveldb::WriteOptions(), key, leveldb::Slice("value1"));
  if (status.ok()) {
    LOG(INFO) << "put key[" << key.ToString() << "] success";
    status = db->Get(leveldb::ReadOptions(), key, &value);
    if (status.ok()) {
      LOG(INFO) << "get key[" << key.ToString() << "] success, value[" << value
                << "]";
    } else {
      LOG(ERROR) << "get key[" << key.ToString() << "] failed, status["
                 << status.ToString() << "]";
    }
  } else {
    LOG(ERROR) << "put key[" << key.ToString() << "] failed, status["
               << status.ToString() << "]";
  }

  // delete a key
  status = db->Delete(leveldb::WriteOptions(), key);
  if (status.ok()) {
    LOG(INFO) << "delete key[" << key.ToString() << "] success";
  } else {
    LOG(ERROR) << "delete key[" << key.ToString() << "] failed, status["
               << status.ToString() << "]";
  }

  // write batch
  leveldb::WriteBatch batch;
  for (int i = 0; i < 11; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    batch.Put(key, value);
  }
  status = db->Write(leveldb::WriteOptions(), &batch);
  if (status.ok()) {
    LOG(INFO) << "write batch success";
    status = db->Get(leveldb::ReadOptions(), "key0", &value);
    if (status.ok()) {
      LOG(INFO) << "get key0 success, value[" << value << "]";
    } else {
      LOG(ERROR) << "get key0 failed, status[" << status.ToString() << "]";
    }
  } else {
    LOG(ERROR) << "write batch failed, status[" << status.ToString() << "]";
  }
  batch.Clear();
  for (int i = 0; i < 11; ++i) {
    std::string key = "key" + std::to_string(i);
    batch.Delete(key);
  }
  status = db->Write(leveldb::WriteOptions(), &batch);
  if (status.ok()) {
    LOG(INFO) << "delete batch success";
    status = db->Get(leveldb::ReadOptions(), "key0", &value);
    if (status.ok()) {
      LOG(INFO) << "get key0 success, value[" << value << "]";
    } else {
      LOG(ERROR) << "get key0 failed, status[" << status.ToString() << "]";
    }
  } else {
    LOG(ERROR) << "delete batch failed, status[" << status.ToString() << "]";
  }
  batch.Clear();
  for (int i = 0; i < 11; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    batch.Put(key, value);
  }
  status = db->Write(leveldb::WriteOptions(), &batch);

  // iterator
  LOG(INFO) << "------------ iterator ------------";
  leveldb::ReadOptions read_options;
  read_options.fill_cache = false;
  auto* iter = db->NewIterator(read_options);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    LOG(INFO) << "forward iter: key[" << iter->key().ToString() << "], value["
              << iter->value().ToString() << "]";
  }
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    LOG(INFO) << "backward iter: key[" << iter->key().ToString() << "], value["
              << iter->value().ToString() << "]";
  }
  for (iter->Seek("key5"); iter->Valid() && iter->key().ToString() < "key8";
       iter->Next()) {
    LOG(INFO) << "range iter: key[" << iter->key().ToString() << "], value["
              << iter->value().ToString() << "]";
  }
  delete iter;

  delete db;

  return 0;
}