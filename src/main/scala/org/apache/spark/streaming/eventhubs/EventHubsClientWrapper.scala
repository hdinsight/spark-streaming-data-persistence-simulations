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
package org.apache.spark.streaming.eventhubs

import java.time.Instant
import com.microsoft.azure.eventhubs.EventData.SystemProperties
import com.microsoft.azure.eventhubs._
import com.microsoft.azure.servicebus.amqp.AmqpConstants
import org.mockito.internal.util.reflection.Whitebox
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import java.util.Random
import org.apache.commons.lang.RandomStringUtils

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
class EventHubsClientWrapper extends Serializable {

  def createReceiver(eventhubsParams: Map[String, String],
                     partitionId: String,
                     offsetStore: OffsetStore,
                     maximumEventRate: Int
                    ): Unit = {

    //Determine the offset to start receiving data

    var offsetType = EventhubsOffsetType.None

    val previousOffset = offsetStore.read()

    if(previousOffset != "-1" && previousOffset != null) {

      offsetType = EventhubsOffsetType.PreviousCheckpoint
      this.currentOffset = previousOffset

    } else if (eventhubsParams.contains("eventhubs.filter.offset")) {

      offsetType = EventhubsOffsetType.InputByteOffset
      this.currentOffset = eventhubsParams("eventhubs.filter.offset")

    } else if (eventhubsParams.contains("eventhubs.filter.enqueuetime")) {

      offsetType = EventhubsOffsetType.InputTimeOffset
      this.currentOffset = eventhubsParams("eventhubs.filter.enqueuetime")
    }

    if (eventhubsParams.contains("eventhubs.event.size")) {

      this.eventLength = eventhubsParams("eventhubs.event.size").toInt
    }

    this.MAXIMUM_EVENT_RATE = maximumEventRate

    if (maximumEventRate > 0 && maximumEventRate < this.MINIMUM_PREFETCH_COUNT)
      this.MAXIMUM_PREFETCH_COUNT = this.MINIMUM_PREFETCH_COUNT
    else if (maximumEventRate >= this.MINIMUM_PREFETCH_COUNT && maximumEventRate < this.MAXIMUM_PREFETCH_COUNT)
      this.MAXIMUM_PREFETCH_COUNT = this.MAXIMUM_EVENT_RATE + 1
    else this.MAXIMUM_EVENT_RATE = this.MAXIMUM_PREFETCH_COUNT - 1
  }

  def receive(): Iterable[EventData] = {

    val receivedEvents: ArrayBuffer[EventData] = new ArrayBuffer[EventData]()

    for(i <- 0 until this.randomGenerator.nextInt(this.MAXIMUM_PREFETCH_COUNT)) {

      val eventPayload: String = RandomStringUtils.randomAlphanumeric(this.eventLength)
      val eventData: EventData = new EventData(eventPayload.getBytes())

      val systemPropertiesMap: java.util.HashMap[String, AnyRef] = new java.util.HashMap[String, AnyRef]()

      systemPropertiesMap.put(AmqpConstants.OFFSET_ANNOTATION_NAME, this.currentOffset)
      systemPropertiesMap.put(AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME, Long.box(this.eventSequenceNumber))
      systemPropertiesMap.put(AmqpConstants.PARTITION_KEY_ANNOTATION_NAME, this.eventhubPartitionId)
      systemPropertiesMap.put(AmqpConstants.ENQUEUED_TIME_UTC_ANNOTATION_NAME, Instant.now())

      this.eventSequenceNumber = this.eventSequenceNumber + 1
      this.currentOffset = (this.currentOffset.toLong + 1).toString

      val systemProperties: SystemProperties = new SystemProperties(systemPropertiesMap)

      Whitebox.setInternalState(eventData, "systemProperties", systemProperties)

      receivedEvents += eventData
    }

    receivedEvents
  }

  def close(): Unit = {}

  private val randomGenerator: Random = new Random()
  private var currentOffset: String = PartitionReceiver.START_OF_STREAM
  private var eventSequenceNumber: Long = 0
  private val eventhubPartitionId: String  = "0"
  private var eventLength: Int = 32

  private val MINIMUM_PREFETCH_COUNT: Int = 10
  private var MAXIMUM_PREFETCH_COUNT: Int = 999
  private var MAXIMUM_EVENT_RATE: Int = 0
}
