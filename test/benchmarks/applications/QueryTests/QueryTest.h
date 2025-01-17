#pragma once

#include <iostream>
#include <fstream>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>

#include "utils/TupleSchema.h"
#include "utils/QueryApplication.h"
#include "utils/Utils.h"
#include "benchmarks/applications/BenchmarkQuery.h"
#include "cql/predicates/ComparisonPredicate.h"


class QueryTest : public BenchmarkQuery {
 private:
  struct InputSchema {
    int64_t id;
    int64_t value;
    int64_t payload;
    int64_t timestamp;

    static void parse(InputSchema &tuple, std::string &line) {
      std::istringstream iss(line);
      std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                     std::istream_iterator<std::string>{}};
      tuple.id = std::stol(words[0]);
      tuple.value = std::stol(words[1]);
      tuple.payload = std::stol(words[2]);
      tuple.timestamp = std::stol(words[3]);
    }
  };

 public:
  TupleSchema *m_schema = nullptr;
  QueryApplication *m_application = nullptr;
  std::vector<char> *m_data = nullptr;
  bool m_debug = false;

  QueryApplication *getApplication() override {
    return m_application;
  }

  virtual void createApplication() = 0;

  void loadInMemoryData() {
    // O.K.
    size_t len = SystemConf::getInstance().BUNDLE_SIZE;
    m_data = new std::vector<char>(len);
    auto buf = (InputSchema *) m_data->data();

    unsigned long idx = 0;
    while (idx < len / sizeof(InputSchema)) {
      buf[idx].id = idx;
      buf[idx].value = idx%100;
      buf[idx].payload = idx;
      buf[idx].timestamp = idx;
      idx++;
    }

    // read from file if wanted:
    /*
    std::string filePath = Utils::GetHomeDir() + "/LightSaber/resources/datasets/generated_data/";
    std::ifstream file(filePath + "filter-sink-data.txt");
    std::string line;

    unsigned long idx = 0;
    while (std::getline(file, line) && idx < len / sizeof(InputSchema)) {
      InputSchema::parse(buf[idx], line);
      idx++;
    }
    */
    if (m_debug) {
      std::cout << "id value payload timestamp" << std::endl;
      for (unsigned long i = 0; i < m_data->size() / sizeof(InputSchema); ++i) {
        printf("[DBG] %09d: %7d %13d %8d %13d \n",
               i, buf[i].id, buf[i].value, buf[i].payload, buf[i].timestamp);
      }
    }
  };

  std::vector<char> *getInMemoryData() override {
    return m_data;
  }

  std::vector<char> *getStaticData() override {
    throw std::runtime_error("error: this benchmark does not have static data");
  }

  TupleSchema *getSchema() override {
    if (m_schema == nullptr)
      createSchema();
    return m_schema;
  }

  void createSchema() {
    m_schema = new TupleSchema(4, "QueryTest");
    auto longAttr = AttributeType(BasicType::Long);
// Any idea on how to declare this as uint64_t?
    m_schema->setAttributeType(0, longAttr);  /*      id:  long */
    m_schema->setAttributeType(1, longAttr);  /*      value:  long */
    m_schema->setAttributeType(2, longAttr);  /*      payload:  long */
    m_schema->setAttributeType(3, longAttr);  /*      timestamp:  long */
  }
};
