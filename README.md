# spark-streaming-data-persistence-simulations

Examples showing how Spark streaming applications can be simulated and data persisted to Azure blob, 
Hive table and Azure SQL Table with Azure Servicebus Eventhubs as flow control manager. 

## Usage

### EventhubsEventCount

spark-submit --master yarn-cluster <...SparkConfigurations...>
--class com.microsoft.spark.streaming.simulations.workloads.EventHubsEventCount spark-streaming-data-persistence-simulations-2.0.0.jar
--eventhubs-namespace <EventhubsNamespace> --eventhubs-name <EventhubsName> --eventSizeInChars <EventSizeInChars
--partition-count  $partitionCount --batch-interval-in-seconds <BatchInterval> --checkpoint-directory <CheckpointDirectory>
--event-count-folder <EventCountFolder> --job-timeout-in-minutes $timeoutInMinutes
    
### EventhubsToAzureBlobAsJSON 
spark-submit --master yarn-cluster <...SparkConfigurations...>
--class com.microsoft.spark.streaming.simulations.workloads.EventhubsToAzureBlobAsJSON spark-streaming-data-persistence-simulations-2.0.0.jar
--eventhubs-namespace <EventhubsNamespace> --eventhubs-name <EventhubsName> --eventSizeInChars <EventSizeInChars>
--partition-count  $partitionCount --batch-interval-in-seconds <BatchInterval> --checkpoint-directory <CheckpointDirectory>
--event-count-folder <EventCountFolder> --event-store-folder <EventStoreFolder> --job-timeout-in-minutes <TimeoutInMinutes>
   
### EventhubsToHiveTable

spark-submit --master yarn-cluster <...SparkConfigurations...>
--class com.microsoft.spark.streaming.simulations.workloads.EventhubsToHiveTable spark-streaming-data-persistence-simulations-2.0.0.jar
--eventhubs-namespace <EventhubsNamespace> --eventhubs-name <EventhubsName> --eventSizeInChars <EventSizeInChars>
--partition-count  <PartitionCount> --batch-interval-in-seconds <BatchInterval> --checkpoint-directory <CheckpointDirectory>
--event-count-folder <EventCountFolder> --event-hive-table <EventHiveTable> --job-timeout-in-minutes <TimeoutInMinutes>
   
### EventhubsToSQLTable
spark-submit --master yarn-cluster <...SparkConfigurations...>
--class com.microsoft.spark.streaming.simulations.workloads.EventhubsToSQLTable spark-streaming-data-persistence-simulations-2.0.0.jar
--eventhubs-namespace <EventhubsNamespace> --eventhubs-name <EventhubsName> --eventSizeInChars <EventSizeInChars>
--partition-count  <PartitionCount> --batch-interval-in-seconds <BatchInterval> --checkpoint-directory <CheckpointDirectory>
--event-count-folder <EventCountFolder> --sql-server-fqdn <SqlServerFQDN> --sql-database-name <SqlDatabaseName>
--database-username <DatabaseUsername> --database-password <DatabasePassword> --event-sql-table <EventSQLTable>
 --job-timeout-in-minutes <TimeoutInMinutes>

## Example:
      spark-submit --master yarn-cluster --class com.microsoft.spark.streaming.simulations.workloads.EventhubsEventCount --num-executors 24 
      --executor-memory 2G --executor-cores 1 --driver-memory 4G spark-streaming-data-persistence-simulations-2.0.0.jar 
      --eventhubs-namespace 'sparkeventhubswestus' --eventhubs-name 'eventhubs8westus' --partition-count 8 --batch-interval-in-seconds 15 
      --checkpoint-directory 'hdfs://mycluster/EventCheckpoint-15-8-16' --event-count-folder '/EventCount-15-8-16/EventCount15' 
      --job-timeout-in-minutes -1

### Note:
1. Use any string for --eventhubs-namespace and --eventhubs-name for launching the Spark streaming application.
2. Use the same strings to restart the Spark streaming application to use the saved checkpoint.
3. Use any integer for --partition-count and use at least double that number for --num-executors

## Build Prerequisites

In order to build and run the examples, you need to have:

1. Java 1.8 SDK.
2. Maven 3.x
3. Scala 2.11

## Build Command
    mvn clean
    mvn package
