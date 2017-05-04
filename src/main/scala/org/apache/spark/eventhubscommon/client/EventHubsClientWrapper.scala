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
package org.apache.spark.eventhubscommon.client

import java.time.Instant
import java.util.Date

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import com.microsoft.azure.eventhubs.{EventHubClient => AzureEventHubClient, _}
import com.microsoft.azure.eventhubs.EventData.SystemProperties
import com.microsoft.azure.servicebus.ConnectionStringBuilder
import com.microsoft.azure.servicebus.amqp.AmqpConstants
import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore
import org.mockito.internal.util.reflection.Whitebox

@SerialVersionUID(1L)
class EventHubsClientWrapper extends Serializable {

  def createReceiver(eventhubsParams: Map[String, String],
                     partitionId: String,
                     startOffset: String,
                     maximumEventRate: Int): Unit = {
    val (connectionString, consumerGroup, receiverEpoch) = configureGeneralParameters(
      eventhubsParams)

    val offsetType = EventhubsOffsetTypes.PreviousCheckpoint
    val currentOffset = startOffset

    MAXIMUM_EVENT_RATE = configureMaxEventRate(maximumEventRate)
  }

  def createReceiver(eventhubsParams: Map[String, String],
                     partitionId: String,
                     offsetStore: OffsetStore,
                     maximumEventRate: Int): Unit = {

    // Determine the offset to start receiving data

    var offsetType = EventhubsOffsetTypes.None

    val previousOffset = offsetStore.read()

    if(previousOffset != "-1" && previousOffset != null) {
      offsetType = EventhubsOffsetTypes.PreviousCheckpoint
      currentOffset = previousOffset
    } else if (eventhubsParams.contains("eventhubs.filter.offset")) {
      offsetType = EventhubsOffsetTypes.InputByteOffset
      currentOffset = eventhubsParams("eventhubs.filter.offset")
    } else if (eventhubsParams.contains("eventhubs.filter.enqueuetime")) {
      offsetType = EventhubsOffsetTypes.InputTimeOffset
      currentOffset = eventhubsParams("eventhubs.filter.enqueuetime")
    }

    if (eventhubsParams.contains("eventhubs.event.size")) {
      eventLength = eventhubsParams("eventhubs.event.size").toInt
    }

    MAXIMUM_EVENT_RATE = configureMaxEventRate(maximumEventRate)
  }

  def receive(expectedEventCount: Int = Random.nextInt(MAXIMUM_PREFETCH_COUNT)):
  Iterable[EventData] = {

    val receivedEvents: ArrayBuffer[EventData] = new ArrayBuffer[EventData]()

    for(i <- 0 until expectedEventCount) {

      val eventPayload: String = Random.alphanumeric.take(eventLength).mkString
      val eventData: EventData = new EventData(eventPayload.getBytes())

      val systemPropertiesMap: java.util.HashMap[String, AnyRef] =
        new java.util.HashMap[String, AnyRef]()

      systemPropertiesMap.put(AmqpConstants.OFFSET_ANNOTATION_NAME, currentOffset)
      systemPropertiesMap.put(AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME,
        Long.box(eventSequenceNumber))
      systemPropertiesMap.put(AmqpConstants.PARTITION_KEY_ANNOTATION_NAME, eventhubPartitionId)
      systemPropertiesMap.put(AmqpConstants.ENQUEUED_TIME_UTC_ANNOTATION_NAME,
        Date.from(Instant.now()))

      eventSequenceNumber = eventSequenceNumber + 1
      currentOffset = (currentOffset.toLong + 1).toString

      val systemProperties: SystemProperties = new SystemProperties(systemPropertiesMap)

      Whitebox.setInternalState(eventData, "systemProperties", systemProperties)

      receivedEvents += eventData
    }

    receivedEvents
  }

  def close(): Unit = {}

  private def configureGeneralParameters(eventhubsParams: Map[String, String]) = {
    if (eventhubsParams.contains("eventhubs.uri") &&
      eventhubsParams.contains("eventhubs.namespace")) {
      throw new IllegalArgumentException(s"Eventhubs URI and namespace cannot both be specified" +
        s" at the same time.")
    }

    val namespaceName = if (eventhubsParams.contains("eventhubs.namespace")) {
      eventhubsParams.get("eventhubs.namespace")
    } else {
      eventhubsParams.get("eventhubs.uri")
    }
    if (namespaceName.isEmpty) {
      throw new IllegalArgumentException(s"Either Eventhubs URI or namespace nust be" +
        s" specified.")
    }
    // TODO: validate inputs
    val evhName = eventhubsParams("eventhubs.name")
    val evhPolicyName = eventhubsParams.getOrElse("eventhubs.policyname", "eventhubspolicyname")
    val evhPolicyKey = eventhubsParams.getOrElse("eventhubs.policykey", "eventhubspolicykey")
    val connectionString = new ConnectionStringBuilder(namespaceName.get, evhName, evhPolicyName,
      evhPolicyKey)
    // Set the consumer group if specified.
    val consumerGroup = eventhubsParams.getOrElse("eventhubs.consumergroup",
      AzureEventHubClient.DEFAULT_CONSUMER_GROUP_NAME)
    // Set the epoch if specified
    val receiverEpoch = eventhubsParams.getOrElse("eventhubs.epoch",
      DEFAULT_RECEIVER_EPOCH.toString).toLong
    (connectionString, consumerGroup, receiverEpoch)
  }

  private def configureMaxEventRate(userDefinedEventRate: Int): Int = {
    if (userDefinedEventRate > 0 && userDefinedEventRate < MINIMUM_PREFETCH_COUNT) {
      MAXIMUM_PREFETCH_COUNT = MINIMUM_PREFETCH_COUNT
    } else if (userDefinedEventRate >= MINIMUM_PREFETCH_COUNT &&
      userDefinedEventRate < MAXIMUM_PREFETCH_COUNT) {
      MAXIMUM_PREFETCH_COUNT = userDefinedEventRate + 1
    } else {
      MAXIMUM_EVENT_RATE = MAXIMUM_PREFETCH_COUNT - 1
    }
    MAXIMUM_EVENT_RATE
  }

  private var currentOffset: String = PartitionReceiver.START_OF_STREAM
  private var eventSequenceNumber: Long = 0
  private val eventhubPartitionId: String = "0"
  private var eventLength: Int = 32

  private val MINIMUM_PREFETCH_COUNT: Int = 10
  private var MAXIMUM_PREFETCH_COUNT: Int = 999
  private var MAXIMUM_EVENT_RATE: Int = 0
  private val DEFAULT_RECEIVER_EPOCH = -1L
}

object EventHubsClientWrapper {

  def getEventHubReceiver(eventhubsParams: Map[String, String],
                           partitionId: Int,
                           startOffset: Long,
                           maximumEventRate: Int): EventHubsClientWrapper = {

    // TODO: reuse client
    val eventHubClientWrapperInstance = new EventHubsClientWrapper()
    eventHubClientWrapperInstance.createReceiver(eventhubsParams, partitionId.toString,
      startOffset.toString, maximumEventRate)
    eventHubClientWrapperInstance
  }
}