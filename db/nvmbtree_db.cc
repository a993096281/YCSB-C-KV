//
// 
//
#include <iostream>


#include "nvmbtree_db.h"
#include "lib/coding.h"

using namespace std;

namespace ycsbc {
    NvmBtree::NvmBtree(const char *dbfilename, utils::Properties &props) :noResult(0){
    
        //set option
        SetOptions(props);
        if(AllocatorInit(path, nvm_size, valuepath, nvm_value_size) < 0) {
            printf("Initial allocator failed\n");
        }
        

        db_ = new NVMBtree();
        if(!db_) {
            printf("creat nvmlevelhash error\n");
            AllocatorExit();
        }
    }

    void NvmBtree::SetOptions(utils::Properties &props) {
        path = "/pmem/key";
        valuepath = "/pmem/value";
        nvm_size = 100 * (1ULL << 30);
        nvm_value_size = 200 * (1ULL << 30);
        key_size = NVM_KeySize;
        value_size = NVM_ValueSize;
        

    }


    int NvmBtree::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        uint64_t keyu = char8toint64(key.c_str());
        value = db_->Get(keyu);
        if(value.empty()){
            noResult++;
        }
        return DB::kOK;
    }


    int NvmBtree::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        std::vector<std::string> values;
        uint64_t keyu = char8toint64(key.c_str());
        uint64_t max = ~0;
        //printf("max:%lu\n",max);
        db_->GetRange(keyu, max, values, len);
        //printf("scan:%ld,%d\n",values.size(),len);
        return DB::kOK;
    }

    int NvmBtree::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        string value;
        value.append(value_size, 'a');
        uint64_t keyu = char8toint64(key.c_str());
        db_->Insert(keyu, value);
        return DB::kOK;
    }

    int NvmBtree::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int NvmBtree::Delete(const std::string &table, const std::string &key) {
        uint64_t keyu = char8toint64(key.c_str());
        db_->Delete(keyu);
        return DB::kOK;
    }

    void NvmBtree::PrintStats() {
        //if(noResult != 0) {
            cout<<"read not found:"<<noResult<<endl;
        //}
    }

    NvmBtree::~NvmBtree() {
        delete db_;
        AllocatorExit();
    }

    void NvmBtree::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void NvmBtree::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
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
