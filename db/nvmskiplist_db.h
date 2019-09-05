//
// 
//

#ifndef YCSB_C_NVMSKIPLIST_DB_H
#define YCSB_C_NVMSKIPLIST_DB_H

#include "core/db.h"
#include <iostream>
#include <string>
#include "core/properties.h"
#include <nvmskiplist/persistent_skiplist_wrapper.h>

using std::cout;
using std::endl;

namespace ycsbc {
    class NvmSkiplist : public DB{
    public :
        NvmSkiplist(const char *dbfilename, utils::Properties &props);
        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);


        int Delete(const std::string &table, const std::string &key);

        void PrintStats();

        ~NvmSkiplist();

    private:
        rocksdb::PersistentSkiplistWrapper *db_;
        unsigned noResult;

        void SetOptions(utils::Properties &props);
        void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
        void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);

        std::string path;
        std::string valuepath;
        size_t nvm_size;
        size_t nvm_value_size;
        size_t key_size;
        size_t value_size;

    };
}


#endif //YCSB_C_NVMSKIPLIST_DB_H