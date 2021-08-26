#include "cql/operators/AggregationType.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "utils/Query.h"
#include "benchmarks/applications/ClusterMonitoring/ClusterMonitoring.h"

class CM1 : public ClusterMonitoring {
 private:
  void createApplication() override {
    SystemConf::getInstance().SLOTS = 256;
    SystemConf::getInstance().PARTIAL_WINDOWS = 288; // change this depending on the batch size
    SystemConf::getInstance().HASH_TABLE_SIZE = 8;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;

    // Configure first query
    std::vector<AggregationType> aggregationTypes(1);
    aggregationTypes[0] = AggregationTypes::fromString("sum");

    // summe über etwas

    std::vector<ColumnReference *> aggregationAttributes(1);
    aggregationAttributes[0] = new ColumnReference(8, BasicType::Float);

    // Float Col. CPU

    std::vector<Expression *> groupByAttributes(1);
    groupByAttributes[0] = new ColumnReference(6, BasicType::Integer);

    // Groub By integer category

    auto window = new WindowDefinition(RANGE_BASED, 60, 1); // (RANGE_BASED, 60*25, 1*25)
    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

    // Window: RANGE_BASED is a value of an enum, 60: size, 1: Slide
    // Aggregation: Use a range-based window of size 60 and slide 1 to make a sum over Column 8 beig float "CPU" and group Attributes by Column 6 being int Category

    bool replayTimestamps = window->isRangeBased();

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge);
    genCode->setInputSchema(getSchema());
    genCode->setAggregation(aggregation);
    genCode->setQueryId(0);
    genCode->setup();
    OperatorCode *cpuCode = genCode;

    // Print operator
    std::cout << cpuCode->toSExpr() << std::endl;

    auto queryOperator = new QueryOperator(*cpuCode);
    std::vector<QueryOperator *> operators;
    operators.push_back(queryOperator);

    // used for latency measurements
    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0,
                                         operators,
                                         *window,
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
  CM1(bool inMemory = true) {
    m_name = "CM1";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};
