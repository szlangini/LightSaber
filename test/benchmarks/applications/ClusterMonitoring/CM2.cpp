#include "cql/operators/AggregationType.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "cql/expressions/IntConstant.h"
#include "utils/Query.h"
#include "cql/operators/Selection.h"
#include "benchmarks/applications/ClusterMonitoring/ClusterMonitoring.h"

class CM2 : public ClusterMonitoring {
 private:
  void createApplication() override {

    SystemConf::getInstance().SLOTS = 256;
    SystemConf::getInstance().PARTIAL_WINDOWS = 256;
    SystemConf::getInstance().HASH_TABLE_SIZE = 32;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;

    // Configure first query
    auto predicate = new ComparisonPredicate(EQUAL_OP, new ColumnReference(4), new IntConstant(3));
    Selection *selection = new Selection(predicate);

    // check if equal: eventType == IntConstant(3)
    // Filter based on this predicate

    // Configure second query
    std::vector<AggregationType> aggregationTypes(1);
    aggregationTypes[0] = AggregationTypes::fromString("sum");

    // Sum Aggregation


    std::vector<ColumnReference *> aggregationAttributes(1);
    aggregationAttributes[0] = new ColumnReference(8, BasicType::Float);

    // Aggregation on Float CPU

    std::vector<Expression *> groupByAttributes(1);
    groupByAttributes[0] = new ColumnReference(1, BasicType::Long);

    // Grouped by long jobId

    auto window = new WindowDefinition(RANGE_BASED, 60, 1); //ROW_BASED, 60*25, 1*25);
    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

    // Range-based, sliding window with size 60 and slide 1
    // Aggregation: Use a range-based window of size 60 and slide 1 to make a sum over Column 8 being float "CPU" and group Attributes by Column 1 being long jobId

    bool replayTimestamps = window->isRangeBased();

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge);
    genCode->setInputSchema(getSchema());
    genCode->setSelection(selection); // seems like they belong together
    genCode->setAggregation(aggregation);
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
  CM2(bool inMemory = true) {
    m_name = "CM2";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};
