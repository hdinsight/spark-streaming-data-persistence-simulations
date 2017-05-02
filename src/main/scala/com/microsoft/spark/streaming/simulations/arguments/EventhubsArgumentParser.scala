/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.streaming.simulations.arguments

object EventhubsArgumentParser {

  type ArgumentMap = Map[Symbol, Any]

  def usageExample(): Unit = {

    val eventhubsNamespace: String = "sparkstreamingeventhub-ns"
    val eventhubsName: String = "sparkstreamingeventhub"
    val eventSizeInChars: Int = 32
    val partitionCount: Int = 32
    val batchInterval: Int = 10
    val checkpointDirectory: String = "/EventCheckpoint10"
    val progressDirectory: String = "/EventProgress10"
    val eventCountFolder: String = "/EventCount/EventCount10"
    val eventStoreFolder: String = "/EventStore/EventStore10"
    val eventHiveTable: String = "EventHiveTable10"
    val sqlServerFQDN: String = "servername.database.windows.net"
    val sqlDatabaseName: String = "databasename"
    val databaseUsername: String = "[DatabaseUsername]"
    val databasePassword: String = "[DatabasePassword]"
    val eventSQLTable: String = "EventSQLTable10"
    val timeoutInMinutes: Long = -1

    println()
    println(s"Usage [EventhubsEventCount]: spark-submit --master yarn-cluster ..." +
      s" --class com.microsoft.spark.streaming.simulations.workloads" +
      s".receiverstreaming.EventHubsEventCount " +
      s"/home/hdiuser/spark-streaming-data-persistence-simulations-2.1.0.jar " +
      s"--eventhubs-namespace \'$eventhubsNamespace\' --eventhubs-name \'$eventhubsName\' " +
      s" --partition-count  $partitionCount --eventSizeInChars $eventSizeInChars " +
      s"--batch-interval-in-seconds $batchInterval --checkpoint-directory" +
      s" \'$checkpointDirectory\' --event-count-folder \'$eventCountFolder\' " +
      s"--job-timeout-in-minutes $timeoutInMinutes")
    println()
    println(s"Usage [EventhubsToAzureBlobAsJSON]: spark-submit --master yarn-cluster ..." +
      s" --class com.microsoft.spark.streaming.simulations.workloads.receiverstreaming" +
      s".EventhubsToAzureBlobAsJSON " +
      s"/home/hdiuser/spark-streaming-data-persistence-simulations-2.1.0.jar " +
      s"--eventhubs-namespace \'$eventhubsNamespace\' --eventhubs-name \'$eventhubsName\' " +
      s" --partition-count  $partitionCount --eventSizeInChars $eventSizeInChars " +
      s"--batch-interval-in-seconds $batchInterval --checkpoint-directory" +
      s" \'$checkpointDirectory\' --event-count-folder \'$eventCountFolder\' --event-store-folder" +
      s" \'$eventStoreFolder\' --job-timeout-in-minutes $timeoutInMinutes")
    println()
    println(s"Usage [EventhubsToHiveTable]: spark-submit --master yarn-cluster ..." +
      s" --class com.microsoft.spark.streaming.simulations.workloads.receiverstreaming" +
      s".EventhubsToHiveTable " +
      s"/home/hdiuser/spark-streaming-data-persistence-simulations-2.1.0.jar " +
      s"--eventhubs-namespace \'$eventhubsNamespace\' --eventhubs-name \'$eventhubsName\' " +
      s" --partition-count  $partitionCount --eventSizeInChars $eventSizeInChars " +
      s"--batch-interval-in-seconds $batchInterval --checkpoint-directory" +
      s" \'$checkpointDirectory\' --event-count-folder \'$eventCountFolder\' --event-hive-table" +
      s" \'$eventHiveTable\' --job-timeout-in-minutes $timeoutInMinutes")
    println()
    println(s"Usage [EventhubsToSQLTable]: spark-submit --master yarn-cluster ..." +
      s" --class com.microsoft.spark.streaming.simulations.workloads.receiverstreaming" +
      s".EventhubsToSQLTable " +
      s"/home/hdiuser/spark-streaming-data-persistence-simulations-2.1.0.jar " +
      s"--eventhubs-namespace \'$eventhubsNamespace\' --eventhubs-name \'$eventhubsName\' " +
      s" --partition-count $partitionCount --eventSizeInChars $eventSizeInChars " +
      s"--batch-interval-in-seconds $batchInterval --checkpoint-directory" +
      s" \'$checkpointDirectory\' --event-count-folder \'$eventCountFolder\' " +
      s"--sql-server-fqdn \'$sqlServerFQDN\' --sql-database-name \'$sqlDatabaseName\' " +
      s"--database-username \'$databaseUsername\' --database-password \'$databasePassword\' " +
      s"--event-sql-table \'$eventSQLTable\' --job-timeout-in-minutes $timeoutInMinutes")
    println()
    println()
    println(s"Usage [EventhubsEventCount]: spark-submit --master yarn-cluster ..." +
      s" --class com.microsoft.spark.streaming.simulations.workloads" +
      s".directstreaming.EventHubsEventCount " +
      s"/home/hdiuser/spark-streaming-data-persistence-simulations-2.1.0.jar " +
      s"--eventhubs-namespace \'$eventhubsNamespace\' --eventhubs-name \'$eventhubsName\' " +
      s" --partition-count  $partitionCount --eventSizeInChars $eventSizeInChars " +
      s"--batch-interval-in-seconds $batchInterval --checkpoint-directory" +
      s" \'$checkpointDirectory\' --progress-directory \'$progressDirectory\' " +
      s"--event-count-folder \'$eventCountFolder\' --job-timeout-in-minutes $timeoutInMinutes")
    println()
  }

  def parseArguments(argumentMap : ArgumentMap, argumentList: List[String]) : ArgumentMap = {

    argumentList match {
      case Nil => argumentMap
      case "--eventhubs-namespace" :: value:: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventhubsNamespace) ->
          value.toString), tail)
      case "--eventhubs-name" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventhubsName) ->
          value.toString), tail)
      case "--event-size-in-Chars" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventSizeInChars) ->
          value.toInt), tail)
      case "--event-max-rate-per-partition" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventMaxRatePerPartition) ->
          value.toInt), tail)
      case "--partition-count" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.PartitionCount) ->
          value.toInt), tail)
      case "--batch-interval-in-seconds" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds) ->
          value.toInt), tail)
      case "--checkpoint-directory" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.CheckpointDirectory) ->
          value.toString), tail)
      case "--progress-directory" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.ProgressDirectory) ->
          value.toString), tail)
      case "--event-count-folder" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventCountFolder) ->
          value.toString), tail)
      case "--event-store-folder" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventStoreFolder) ->
          value.toString), tail)
      case "--event-hive-table" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventHiveTable) ->
          value.toString), tail)
      case "--sql-server-fqdn" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.SQLServerFQDN) ->
          value.toString), tail)
      case "--sql-database-name" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.SQLDatabaseName) ->
          value.toString), tail)
      case "--database-username" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.DatabaseUsername) ->
          value.toString), tail)
      case "--database-password" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.DatabasePassword) ->
          value.toString), tail)
      case "--event-sql-table" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventSQLTable) ->
          value.toString), tail)
      case "--job-timeout-in-minutes"  :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.TimeoutInMinutes) ->
          value.toLong), tail)
      case option :: tail =>
        println()
        println("Unknown option: " + option)
        println()
        usageExample()
        sys.exit(1)
    }
  }

  def verifyEventhubsEventCountArguments(argumentMap : ArgumentMap): Unit = {

    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.EventhubsNamespace)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.EventhubsName)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.PartitionCount)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.CheckpointDirectory)))

    assert(argumentMap(Symbol(EventhubsArgumentKeys.PartitionCount)).asInstanceOf[Int] > 0)
    assert(argumentMap(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int] > 0)
  }

  def verifyEventhubsToAzureBlobAsJSONArguments(argumentMap : ArgumentMap): Unit = {

    verifyEventhubsEventCountArguments(argumentMap)

    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.EventStoreFolder)))
  }

  def verifyEventhubsToHiveTableArguments(argumentMap : ArgumentMap): Unit = {

    verifyEventhubsEventCountArguments(argumentMap)

    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.EventHiveTable)))
  }

  def verifyEventhubsToSQLTableArguments(argumentMap : ArgumentMap): Unit = {

    verifyEventhubsEventCountArguments(argumentMap)

    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.SQLServerFQDN)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.SQLDatabaseName)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.DatabaseUsername)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.DatabasePassword)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.EventSQLTable)))
  }
}
