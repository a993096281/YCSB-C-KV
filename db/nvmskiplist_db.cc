//
// 
//
#include <iostream>


#include "nvmskiplist_db.h"
#include "lib/coding.h"

using namespace std;

namespace ycsbc {
    
    NvmSkiplist::NvmSkiplist(const char *dbfilename, utils::Properties &props) :noResult(0){
    
        //set option
        SetOptions(props);
    
        db_ = new rocksdb::PersistentSkiplistWrapper();
        db_->Initialize(path, nvm_size, valuepath, nvm_value_size, 15, 4);
        
    }

    void NvmSkiplist::SetOptions(utils::Properties &props) {
        path = "/pmem/key";
        valuepath = "/pmem/value";
        nvm_size = 100 * (1ULL << 30);
        nvm_value_size = 200 * (1ULL << 30);
        key_size = rocksdb::NVM_KeySize;
        value_size = rocksdb::NVM_ValueSize;
        
    }


    int NvmSkiplist::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        value = db_->Get(key);
        if(value.empty()){
            noResult++;
        }
        return DB::kOK;
    }


    int NvmSkiplist::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        std::vector<std::string> values;
        //printf("max:%lu\n",max);
        db_->GetRange(key, "", values, len);
        //printf("scan:%ld,%d\n",values.size(),len);
        return DB::kOK;
    }

    int NvmSkiplist::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        string value;
        value.append(value_size, 'a');
        db_->Insert(key, value);
        return DB::kOK;
    }

    int NvmSkiplist::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int NvmSkiplist::Delete(const std::string &table, const std::string &key) {
        db_->Delete(key);
        return DB::kOK;
    }

    void NvmSkiplist::PrintStats() {
        //if(noResult != 0) {
            cout<<"read not found:"<<noResult<<endl;
        //}
        db_->PrintStorage();
        db_->PrintInfo();
    }

    NvmSkiplist::~NvmSkiplist() {
        delete db_;
    }

    void NvmSkiplist::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void NvmSkiplist::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
        uint64_t offset = 0;
        uint64_t kv_num = 0;
        uint64_t key_size = 0;
        uint64_t value_size = 0;

        kv_num = DecodeFixed64(value.c_str());
        offset += 8;
        for( unsigned int i = 0; i < kv_num; i++){
            ycsbc::DB::KVPair pair;
            key_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.first.assign(value.c_str() + offset, key_size);
            offset += key_size;

            value_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.second.assign(value.c_str() + offset, value_size);
            offset += value_size;
            kvs.push_back(pair);
        }
    }
}
