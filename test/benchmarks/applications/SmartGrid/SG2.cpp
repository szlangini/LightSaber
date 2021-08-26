#include "cql/operators/AggregationType.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "cql/expressions/IntConstant.h"
#include "utils/Query.h"
#include "benchmarks/applications/SmartGrid/SmartGrid.h"

class SG2 : public SmartGrid {
 private:
  void createApplication() override {
    SystemConf::getInstance().PARTIAL_WINDOWS = 144;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;

    // Configure first query
    std::vector<AggregationType> aggregationTypes(1);
    aggregationTypes[0] = AggregationTypes::fromString("avg");

    std::vector<ColumnReference *> aggregationAttributes(1);
    aggregationAttributes[0] = new ColumnReference(1, BasicType::Float);

    // Average Aggregation over float value column

    std::vector<Expression *> groupByAttributes(3);
    groupByAttributes[0] = new ColumnReference(3, BasicType::Integer);
    groupByAttributes[1] = new ColumnReference(4, BasicType::Integer);
    groupByAttributes[2] = new ColumnReference(5, BasicType::Integer);

    // group by: 1. plug 2. household 3. house

    auto window = new WindowDefinition(RANGE_BASED, 128, 1); //ROW_BASED, 36*1000, 1*1000);
    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

    // Aggregation: Range-based, sliding Window of size 128s and 1s Slide to get the average of values. grouped by 1. plug, 2. household, 3. house


    bool replayTimestamps = window->isRangeBased();

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge);
    genCode->setInputSchema(getSchema());
    genCode->setAggregation(aggregation);
    genCode->setCollisionBarrier(28);
    genCode->setQueryId(0);
    genCode->setup();
    OperatorCode *cpuCode = genCode;

    // Print operator
    std::cout << genCode->toSExpr() << std::endl;

    auto queryOperator = new QueryOperator(*cpuCode);
    std::vector<QueryOperator *> operators;
    operators.push_back(queryOperator);

    // this is used for latency measurements
    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0,
                                         operators,
                                         *window,
                                         m_schema,
                                         m_timestampReference,
                                         true,
                                         replayTimestamps,
                                         false,
                                         useParallelMerge);

    m_application = new QueryApplication(queries);
    m_application->setup();
  }

 public:
  SG2(bool inMemory = true) {
    m_name = "SG2";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};
