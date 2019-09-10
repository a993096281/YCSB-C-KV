//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include <string>
#include "db/basic_db.h"
#include "db/nvmbtree_db.h"
#include "db/nvmnvtree_db.h"

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "nvmbtree") {
    std::string dbpath = props.GetProperty("dbpath","/tmp/test-nvmbtree");
    return new NvmBtree(dbpath.c_str(), props);
  } else if (props["dbname"] == "nvmnvtree") {
    std::string dbpath = props.GetProperty("dbpath","/tmp/test-nvmnvtree");
    return new NvmNvtree(dbpath.c_str(), props);
  } else return NULL;
}

