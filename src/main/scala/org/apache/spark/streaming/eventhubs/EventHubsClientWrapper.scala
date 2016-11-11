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
import scala.util.Random

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
      currentOffset = previousOffset

    } else if (eventhubsParams.contains("eventhubs.filter.offset")) {

      offsetType = EventhubsOffsetType.InputByteOffset
      currentOffset = eventhubsParams("eventhubs.filter.offset")

    } else if (eventhubsParams.contains("eventhubs.filter.enqueuetime")) {

      offsetType = EventhubsOffsetType.InputTimeOffset
      currentOffset = eventhubsParams("eventhubs.filter.enqueuetime")
    }

    if (eventhubsParams.contains("eventhubs.event.size")) {

      eventLength = eventhubsParams("eventhubs.event.size").toInt
    }

    MAXIMUM_EVENT_RATE = maximumEventRate

    if (maximumEventRate > 0 && maximumEventRate < MINIMUM_PREFETCH_COUNT)
      MAXIMUM_PREFETCH_COUNT = MINIMUM_PREFETCH_COUNT
    else if (maximumEventRate >= MINIMUM_PREFETCH_COUNT && maximumEventRate < MAXIMUM_PREFETCH_COUNT)
      MAXIMUM_PREFETCH_COUNT = MAXIMUM_EVENT_RATE + 1
    else MAXIMUM_EVENT_RATE = MAXIMUM_PREFETCH_COUNT - 1
  }

  def receive(): Iterable[EventData] = {

    val receivedEvents: ArrayBuffer[EventData] = new ArrayBuffer[EventData]()

    for(i <- 0 until Random.nextInt(MAXIMUM_PREFETCH_COUNT)) {

      val eventPayload: String = Random.alphanumeric.take(eventLength).mkString
      val eventData: EventData = new EventData(eventPayload.getBytes())

      val systemPropertiesMap: java.util.HashMap[String, AnyRef] = new java.util.HashMap[String, AnyRef]()

      systemPropertiesMap.put(AmqpConstants.OFFSET_ANNOTATION_NAME, currentOffset)
      systemPropertiesMap.put(AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME, Long.box(eventSequenceNumber))
      systemPropertiesMap.put(AmqpConstants.PARTITION_KEY_ANNOTATION_NAME, eventhubPartitionId)
      systemPropertiesMap.put(AmqpConstants.ENQUEUED_TIME_UTC_ANNOTATION_NAME, Instant.now())

      eventSequenceNumber = eventSequenceNumber + 1
      currentOffset = (currentOffset.toLong + 1).toString

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
