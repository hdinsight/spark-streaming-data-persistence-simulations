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

package com.microsoft.spark.streaming.simulations.workloads.directstreaming

import com.microsoft.spark.streaming.simulations.arguments.{EventhubsArgumentKeys, EventhubsArgumentParser}
import com.microsoft.spark.streaming.simulations.arguments.EventhubsArgumentParser._
import com.microsoft.spark.streaming.simulations.common.StreamStatistics

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object EventhubsEventCount {

  def createStreamingContext(inputOptions: ArgumentMap): StreamingContext = {

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
      "eventhubs.progress.dir" ->
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


    /**
      * In Spark 2.0.x, SparkConf must be initialized through EventhubsUtil so that required
      * data structures internal to Azure Eventhubs Client get registered with the Kryo Serializer.
      */

    val sparkConfiguration : SparkConf = EventHubsUtils.initializeSparkStreamingConfigurations

    sparkConfiguration.setAppName(this.getClass.getSimpleName)

    val sparkSession : SparkSession =
      SparkSession.builder().config(sparkConfiguration).getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext,
      Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int]))
    streamingContext.checkpoint(inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory))
      .asInstanceOf[String])

    val eventhubsNameParametersMap: Map[String, Map[String, String]] = Map(eventHubsParameters
    ("eventhubs.name") -> eventHubsParameters)

    val eventHubsStream = EventHubsUtils.createDirectStreams(streamingContext,
      eventHubsParameters("eventhubs.namespace"), eventHubsParameters("eventhubs.progress.dir"),
      eventhubsNameParametersMap)

    val eventHubsWindowedStream = eventHubsStream
      .window(Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds))
        .asInstanceOf[Int]))

    // Count number of events received the past batch

    val batchEventCount = eventHubsWindowedStream.count()

    batchEventCount.print()

    // Count number of events received so far

    val totalEventCountDStream =
      eventHubsWindowedStream.map(m => (StreamStatistics.streamLengthKey, 1L))
    val totalEventCount =
      totalEventCountDStream.updateStateByKey[Long](StreamStatistics.streamLength)

    totalEventCount.checkpoint(Seconds(inputOptions(Symbol(EventhubsArgumentKeys
      .BatchIntervalInSeconds)).asInstanceOf[Int]))

    if (inputOptions.contains(Symbol(EventhubsArgumentKeys.EventCountFolder))) {

      totalEventCount.saveAsTextFiles(inputOptions(Symbol(EventhubsArgumentKeys.EventCountFolder))
        .asInstanceOf[String])
    }

    totalEventCount.print()

    streamingContext
  }

  def main(inputArguments: Array[String]): Unit = {

    val inputOptions: ArgumentMap =
      EventhubsArgumentParser.parseArguments(Map(), inputArguments.toList)

    EventhubsArgumentParser.verifyEventhubsEventCountArguments(inputOptions)

    // Create or recreate streaming context

    val streamingContext = StreamingContext.getOrCreate(inputOptions(Symbol(EventhubsArgumentKeys
      .CheckpointDirectory)).asInstanceOf[String],
      () => createStreamingContext(inputOptions))

    streamingContext.start()

    if(inputOptions.contains(Symbol(EventhubsArgumentKeys.TimeoutInMinutes))) {

      streamingContext.awaitTerminationOrTimeout(inputOptions(Symbol(EventhubsArgumentKeys
        .TimeoutInMinutes)).asInstanceOf[Long] * 60 * 1000)
    }
    else {

      streamingContext.awaitTermination()
    }
  }
}


