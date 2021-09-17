//
// Created by Vincent Szlang on 16.09.21.
//
#include "cql/operators/AggregationType.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "utils/Query.h"
#include "benchmarks/applications/QueryTests/QueryTest.h"


class FS1 : public QueryTest {
 private:
  void createApplication() override {
    // Query::from("input").filter(Attribute("value") > 10000).sink(NullOutputSinkDescriptor::create());

    /* Consider including this t.b.d. with Steffen.
    SystemConf::getInstance().SLOTS = 256;
    SystemConf::getInstance().PARTIAL_WINDOWS = 256;
    SystemConf::getInstance().HASH_TABLE_SIZE = 32;

    */

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON; // Discuss this
    bool replayTimestamps = false; //discuss this

    // Configure first query
    auto predicate = new ComparisonPredicate(GREATER_OP, new ColumnReference(1), new IntConstant(1000000000));
    Selection *selection = new Selection(predicate);

    // Needs a window....
    auto window = new WindowDefinition(RANGE_BASED, 3000, 1); // (RANGE_BASED, 60*25, 1*25)


    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge);
    genCode->setInputSchema(getSchema());
    genCode->setSelection(selection);
    genCode->setQueryId(0);
    genCode->setup();
    OperatorCode *cpuCode = genCode;

    // Print operator
    std::cout << cpuCode->toSExpr() << std::endl;

    auto queryOperator = new QueryOperator(*cpuCode);
    std::vector<QueryOperator *> operators;
    operators.push_back(queryOperator);

    // this is used for latency measurements
    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0,
                                         operators,
                                         window,
                                         m_schema,
                                         m_timestampReference,
                                         true,
                                         replayTimestamps,
                                         !replayTimestamps,
                                         useParallelMerge);

    m_application = new QueryApplication(queries);
    m_application->setup();

  }

 public:
  FS1(bool inMemory = true) {
    m_name = "FS1";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};
