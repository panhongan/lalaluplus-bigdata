package com.github.panhongan.bigdata.kafka;

import com.github.panhongan.bigdata.zk.ZKUtils;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author lalaluplus
 * @since 2021.11.10
 */
public class KafkaOffsetUtils {

    private static final Logger logger = LoggerFactory.getLogger(KafkaOffsetUtils.class);

    public static final long INVALID_OFFSET = -1L;

    public static boolean writeOffset(String zkList, String basePath, String topic, String groupId, OffsetRange offset) {
        CuratorFramework zkClient = null;

        try {
            zkClient = ZKUtils.createCuratorFramework(zkList, null);
            if (Objects.nonNull(zkClient)) {
                String offsetPath = basePath + "/" + topic + "/" + groupId + "/" + offset.topic() + "_" + offset.partition();
                String value = offset.fromOffset() + "_" + offset.untilOffset();

                ZKUtils.createPersistNode(zkClient, offsetPath, value.getBytes(StandardCharsets.UTF_8));
                return true;
            }
        } catch (Exception e) {
            logger.warn("", e);
        } finally {
            ZKUtils.closeCuratorFramework(zkClient);
        }

        return false;
    }

    public static List<OffsetRange> readOffset(String zkList, String basePath, String topic, String groupId, int partitionNum) {
        List<OffsetRange> offsetList = new ArrayList<>();
        CuratorFramework zkClient = null;

        try {
            zkClient = ZKUtils.createCuratorFramework(zkList, null);

            for (int partition = 0; partition < partitionNum; ++partition) {
                String offsetPath = basePath + "/" + groupId + "/" + topic + "_" + partition;

                if (ZKUtils.checkExists(zkClient, offsetPath)) {
                    String value = ZKUtils.getNodeData(zkClient, offsetPath);
                    String[] arr = value.split("_");
                    if (Objects.nonNull(arr) && arr.length == 2) {
                        offsetList.add(OffsetRange.create(topic, partition, Long.valueOf(arr[0]), Long.valueOf(arr[1])));
                    }
                } else {
                    offsetList.add(OffsetRange.create(topic, partition, INVALID_OFFSET, INVALID_OFFSET));
                }
            }
        } catch (Exception e) {
            logger.warn("", e);
        } finally {
            ZKUtils.closeCuratorFramework(zkClient);
        }

        return offsetList;
    }

    public static long getPartitionLatestWriteOffset(String brokerLeader, int port, String topic, int partition) {
        long ret = -1L;
        SimpleConsumer consumer = null;

        try {
            consumer = new SimpleConsumer(brokerLeader, port, 100 * 1000, 65536, "default_client");
            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
            requestInfo.put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1));
            kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), "default_client");
            OffsetResponse response = consumer.getOffsetsBefore(request);
            if (response.hasError()) {
                logger.error("failed to fetch data offset, err : {}", response.errorCode(topic, partition));
            } else {
                long[] offsets = response.offsets(topic, partition);
                ret = offsets[0];
            }
        } catch (Exception e) {
            logger.warn("", e);
        } finally {
            KafkaUtils.closeSimpleConsumer(consumer);
        }

        return ret;
    }

    public static Map<Integer, Long> getTopicLatestWriteOffset(String brokerList, String topic) {
        Map<Integer, Long> map = new HashMap<>();

        try {

            List<PartitionMetadata> partitionMetadataList = KafkaUtils.findTopicPartitionMetadata(brokerList, topic);
            if (CollectionUtils.isNotEmpty(partitionMetadataList)) {
                for (PartitionMetadata metadata : partitionMetadataList) {
                    long lastOffset = KafkaOffsetUtils.getPartitionLatestWriteOffset(metadata.leader().host(), metadata.leader().port(), topic, metadata.partitionId());
                    map.put(metadata.partitionId(), lastOffset);
                }
            }
        } catch (Exception e) {
            logger.warn("", e);
        }

        return map;
    }

    public static int compareReadAndWriteOffset(List<OffsetRange> readOffsets, Map<Integer, Long> writeOffsets) {
        for (OffsetRange offset : readOffsets) {
            int part = offset.partition();
            long writeOffset = writeOffsets.get(part);

            if (offset.untilOffset() < writeOffset) {
                return -1;
            } else if (offset.untilOffset() > writeOffset) {
                return 1;
            }
        }

        return 0;
    }

    public static List<OffsetRange> reviseConsumerOffset(List<OffsetRange> readOffsets, Map<Integer, Long> writeOffsets) {
        List<OffsetRange> offsetRangeList = new ArrayList<>();

        for (OffsetRange offset : readOffsets) {
            int part = offset.partition();
            long writeOffset = writeOffsets.get(part);

            if (offset.untilOffset() <= writeOffset) {
                offsetRangeList.add(offset);
            } else {
                offsetRangeList.add(OffsetRange.create(offset.topic(), offset.partition(), writeOffset, writeOffset));
            }
        }

        return offsetRangeList;
    }
}
