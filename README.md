# Benchmark Utils for Structured streaming

we wanted to have performance benchmarks for various scenarios as part of the RocksDb State Storage implementation [SPARK-28120](https://issues.apache.org/jira/browse/SPARK-28120) 

[Jungtaek Lim](https://github.com/HeartSaVioR) pointed me to his github [project](https://github.com/HeartSaVioR/iot-trucking-app-spark-structured-streaming/tree/master/src/main/scala/com/hortonworks/spark/benchmark/streaming) which he has used for his benchmarks.
I have created this project in similar lines for streaming performance scenarios. 

###### Build the project
        sbt assemebly

###### Use RocksDB State Storage
        /usr/lib/spark/bin/spark-submit \
        --class com.qubole.spark.benchmark.streaming.states.StateStoreBenchmarkRunner \
        --driver-memory 2g \
        --executor-memory 7g \
        --num-executors 1 \
        --max-executors 1 \
        --executor-cores 4 \
        --conf spark.executor.memoryOverhead=3g \
        ./build/spark-benchmark.jar \
        --query-status-file "/tmp/queryStatus-rocksdb" \
        --rate-row-per-second "20000" \
        --output-mode "append" \
        --run-time-in-sec 1800 \
        --trigger-interval 60000 \
        --shuffle-partition 8 \
        --use-rocks-db

###### Use Memory State Storage
        /usr/lib/spark/bin/spark-submit \
        --class com.qubole.spark.benchmark.streaming.states.StateStoreBenchmarkRunner \
        --driver-memory 2g \
        --executor-memory 7g \
        --num-executors 1 \
        --max-executors 1 \
        --executor-cores 4 \
        --conf spark.executor.memoryOverhead=3g \
        ./build/spark-benchmark.jar \
        --query-status-file "/tmp/queryStatus-memory" \
        --rate-row-per-second "20000" \
        --output-mode "append" \
        --run-time-in-sec 1800 \
        --trigger-interval 60000 \
        --shuffle-partition 8

###### Analyze the progress
        /usr/lib/spark/bin/spark-submit \
        --class com.qubole.spark.benchmark.streaming.states.AnalyzeProgress \
        --driver-memory 2g \
        --executor-memory 5g \
        --num-executors 2 \
        --max-executors 2 \
        --conf spark.executor.memoryOverhead=1g \
        ./build/spark-benchmark.jar \
        "/tmp/queryStatus-rocksdb"

###### Sample Output 

        |runId                |maxBatchId|TotalProcessedRecordsInMillion|TotalExecutionTimeInSec|AvgExecutionTimeInSec|MaxStateRowsInMillion|maxStateSizeInGB|
        | 3fc89a39-1cc3-46b...|        33|                         30.62|                1638.42|                48.19|                 8.57|            0.68| 


## Acknowledgement

[Jungtaek Lim](https://github.com/HeartSaVioR) for the original work for the structured streaming benchmarking.
