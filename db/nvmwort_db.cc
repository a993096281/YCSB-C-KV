//
// 
//
#include <iostream>


#include "nvmwort_db.h"
#include "lib/coding.h"

using namespace std;

namespace ycsbc {
    static inline uint64_t char8toint64(const char *key) {
        uint64_t value = ((((uint64_t)key[0]) & 0xff) << 56) | ((((uint64_t)key[1]) & 0xff) << 48) |
                ((((uint64_t)key[2]) & 0xff) << 40) | ((((uint64_t)key[3]) & 0xff) << 32) |
                ((((uint64_t)key[4]) & 0xff) << 24) | ((((uint64_t)key[5]) & 0xff) << 16) |
                ((((uint64_t)key[6]) & 0xff) << 8)  | (((uint64_t)key[7]) & 0xff);
        return value;
    }
    
    NvmWort::NvmWort(const char *dbfilename, utils::Properties &props) :noResult(0){
    
        //set option
        SetOptions(props);
        if(AllocatorInit(path, nvm_size, valuepath, nvm_value_size) < 0) {
            printf("Initial allocator failed\n");
        }
        

        db_ = new NVMWort();
        if(!db_) {
            printf("creat nvmlevelhash error\n");
            AllocatorExit();
        }
    }

    void NvmWort::SetOptions(utils::Properties &props) {
        path = "/pmem/key";
        valuepath = "/pmem/value";
        nvm_size = 100 * (1ULL << 30);
        nvm_value_size = 200 * (1ULL << 30);
        key_size = NVM_KeySize;
        value_size = NVM_ValueSize;
        

    }


    int NvmWort::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        uint64_t keyu = char8toint64(key.c_str());
        value = db_->Get(keyu);
        if(value.empty()){
            noResult++;
        }
        return DB::kOK;
    }


    int NvmWort::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        std::vector<std::string> values;
        uint64_t keyu = char8toint64(key.c_str());
        uint64_t max = ~0;
        //printf("max:%lu\n",max);
        db_->GetRange(keyu, max, values, len);
        //printf("scan:%ld,%d\n",values.size(),len);
        return DB::kOK;
    }

    int NvmWort::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        string value;
        value.append(value_size, 'a');
        uint64_t keyu = char8toint64(key.c_str());
        db_->Insert(keyu, value);
        return DB::kOK;
    }

    int NvmWort::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int NvmWort::Delete(const std::string &table, const std::string &key) {
        uint64_t keyu = char8toint64(key.c_str());
        db_->Delete(keyu);
        return DB::kOK;
    }

    void NvmWort::PrintStats() {
        //if(noResult != 0) {
            cout<<"read not found:"<<noResult<<endl;
        //}
    }

    NvmWort::~NvmWort() {
        delete db_;
        AllocatorExit();
    }

    void NvmWort::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void NvmWort::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
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
