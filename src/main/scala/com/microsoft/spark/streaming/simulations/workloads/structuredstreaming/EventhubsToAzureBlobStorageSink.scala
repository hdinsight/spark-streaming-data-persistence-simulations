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

package com.microsoft.spark.streaming.simulations.workloads.structuredstreaming

import com.microsoft.spark.streaming.simulations.arguments.{EventhubsArgumentKeys, EventhubsArgumentParser}
import com.microsoft.spark.streaming.simulations.arguments.EventhubsArgumentParser._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

object EventhubsToAzureBlobStorageSink {

  def main(inputArguments: Array[String]): Unit = {

    val inputOptions: ArgumentMap =
      EventhubsArgumentParser.parseArguments(Map(), inputArguments.toList)

    EventhubsArgumentParser.verifyEventhubsEventCountArguments(inputOptions)

    var eventHubsParameters = Map[String, String](

      "eventhubs.namespace" ->
        inputOptions(Symbol(EventhubsArgumentKeys.EventhubsNamespace)).asInstanceOf[String],
      "eventhubs.name" ->
        inputOptions(Symbol(EventhubsArgumentKeys.EventhubsName)).asInstanceOf[String],
      "eventhubs.partition.count" ->
        inputOptions(Symbol(EventhubsArgumentKeys.PartitionCount)).asInstanceOf[Int].toString,
      "eventhubs.checkpoint.interval" ->
        inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds))
          .asInstanceOf[Int].toString,
      "eventhubs.checkpoint.dir" ->
        inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).asInstanceOf[String],
      "eventhubs.progressTrackingDir" ->
        inputOptions(Symbol(EventhubsArgumentKeys.ProgressDirectory)).asInstanceOf[String]
    )

    eventHubsParameters =
      if (inputOptions.contains(Symbol(EventhubsArgumentKeys.EventSizeInChars))) {
        eventHubsParameters + ("eventhubs.event.size" ->
          inputOptions(Symbol(EventhubsArgumentKeys.EventSizeInChars))
            .asInstanceOf[Int].toString)
      } else eventHubsParameters

    eventHubsParameters =
      if (inputOptions.contains(Symbol(EventhubsArgumentKeys.EventMaxRatePerPartition))) {
        eventHubsParameters + ("eventhubs.maxRate" ->
          inputOptions(Symbol(EventhubsArgumentKeys.EventMaxRatePerPartition))
            .asInstanceOf[Int].toString)
      } else eventHubsParameters

    val sparkSession = SparkSession.builder().getOrCreate()
    val inputStream = sparkSession.readStream.format("eventhubs")
      .options(eventHubsParameters).load()

    val processingTime: String = inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds))
      .asInstanceOf[Int].toString + " seconds"

    val streamingQuery = inputStream.writeStream.outputMode("append")
      .trigger(ProcessingTime(processingTime))
      .option("checkpointLocation", eventHubsParameters("eventhubs.checkpoint.dir"))
      .format("parquet").option("path", inputOptions(Symbol(EventhubsArgumentKeys.EventStoreFolder))
      .asInstanceOf[String]).partitionBy("enqueuedTime").start()

    val timeOutInMinutes: Long =
      if (inputOptions.contains(Symbol(EventhubsArgumentKeys.TimeoutInMinutes))) {
      inputOptions(Symbol(EventhubsArgumentKeys.TimeoutInMinutes)).asInstanceOf[Long]
    } else -1L

    if (timeOutInMinutes > -1L) {
      streamingQuery.awaitTermination(timeOutInMinutes * 60 * 1000)
    }
    else {
      streamingQuery.awaitTermination()
    }
  }
}


