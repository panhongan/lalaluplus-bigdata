package com.github.panhongan.bigdata.spark

import com.github.panhongan.bigdata.kafka.KafkaOffsetUtils
import org.apache.commons.collections4.MapUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * code demo:
 * val stream = StreamingUtils.createDirectStreamIgnoreOffset[K, V](...)
 *
 * // stream.map(..).flatMap(..).reduceByKey(..).saveAsTextFiles(...)
 *
 * StreamingUtils.saveOffset(stream, zkList)
 *
 *
 * @author lalaluplus
 * @since 2021.11.10
 */

object StreamingUtils {
  
  private val logger = LoggerFactory.getLogger(StreamingUtils.getClass)

  /**
   * @param topicSet topic set
   * @param kafkaConfig Map
   * @param streamingContext StreamingContext
   * @tparam K key
   * @tparam V value
   * @return InputDStream[ConsumerRecord[K,V]]
   */
  def createDirectStreamIgnoreOffset[K, V](topicSet : Set[String], 
                                           kafkaConfig : Map[String, Object], 
                                           streamingContext : StreamingContext) : InputDStream[ConsumerRecord[K, V]] = {
    KafkaUtils.createDirectStream[K, V](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[K, V](topicSet, kafkaConfig))
  }
  
  def createDirectStreamByOffset[K, V](zkList : String, 
                                       offsetZkPath : String,
                                       topic : String,
                                       partitionNum : Int,
                                       kafkaConfig : Map[String, String],
                                       streamingContext : StreamingContext,
                                       reuseLastBatchDataWhenLatest : Boolean) : InputDStream[ConsumerRecord[K, V]] = {
    
    val startReadOffsets = scala.collection.mutable.Map[TopicPartition, Long]()
    
    // latest read offset from zk
    val latestReadOffsets = KafkaOffsetUtils.readOffset(zkList,
      offsetZkPath, 
      kafkaConfig.get(ConsumerConfig.GROUP_ID_CONFIG).get,
      topic,
      partitionNum)
    if (latestReadOffsets.isEmpty) for (i <- 0 until partitionNum) {
      latestReadOffsets.add(OffsetRange.create(topic, i, KafkaOffsetUtils.INVALID_OFFSET, KafkaOffsetUtils.INVALID_OFFSET))
    }

    val latestReadOffsetsScala = latestReadOffsets.asScala

    for (offset <- latestReadOffsetsScala) {
      logger.info("start from, topic = " + offset.topic +
        ", partition = " + offset.partition +
        ", start_offset = " + offset.fromOffset +
        ", end_offset = " + offset.untilOffset)
    }
    
    // latest write offset
    val latestWriteOffsets = KafkaOffsetUtils.getTopicLatestWriteOffset(kafkaConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).get, topic)
    if (MapUtils.isEmpty(latestWriteOffsets)) {
      logger.warn("failed to get partition write offset, topic = {}", topic)
      throw new RuntimeException("No write offset")
    }
    
    val compare = KafkaOffsetUtils.compareReadAndWriteOffset(latestReadOffsets, latestWriteOffsets)
    if (compare == 0) { // largest offset
      if (reuseLastBatchDataWhenLatest) for (offset <- latestReadOffsetsScala) {
        startReadOffsets.put(new TopicPartition(offset.topic, offset.partition), offset.fromOffset)
      } else for (offset <- latestReadOffsetsScala) {
        startReadOffsets.put(new TopicPartition(offset.topic, offset.partition), offset.untilOffset)
      }
    } else if (compare < 0) {
      for (offset <- latestReadOffsetsScala) {
        startReadOffsets.put(new TopicPartition(offset.topic, offset.partition), offset.untilOffset)
      }
    } else if (compare > 0) { // consumer offset > producer offset (invalid)
      val revisedLatestReadOffsets = KafkaOffsetUtils.reviseConsumerOffset(latestReadOffsets, latestWriteOffsets)
      for (offset <- revisedLatestReadOffsets.asScala) {
        startReadOffsets.put(new TopicPartition(offset.topic, offset.partition), offset.untilOffset)
      }
    }
    
    // create stream
    val kafkaStream = KafkaUtils.createDirectStream[K, V](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[K, V](Set(topic), kafkaConfig, startReadOffsets))

    kafkaStream
  }
  
  def saveOffset[K, V](kafkaStream : InputDStream[ConsumerRecord[K, V]], zkList : String, offsetZkPath : String, topic: String, groupId : String) {
    kafkaStream.foreachRDD(rdd => {
        val offsetArr = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offset <- offsetArr) {
          val ret = KafkaOffsetUtils.writeOffset(zkList, offsetZkPath, topic, groupId, offset)
          if (!ret)
            logger.warn("write offset failed : topic = " + topic + ", group_id = " + groupId + ", offset = " + offset)
          else
            logger.info("write offset succeed : topic = " + topic + ", group_id = " + groupId + ", offset = " + offset)
        }
    })
  }
}
