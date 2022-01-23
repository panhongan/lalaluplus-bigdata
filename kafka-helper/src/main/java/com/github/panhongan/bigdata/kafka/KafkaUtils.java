package com.github.panhongan.bigdata.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.github.panhongan.bigdata.zk.ZKUtils;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lalaluplus
 * @since 2021.11.10
 */
public class KafkaUtils {

    private static Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

    private static final int DEFAULT_TIMEOUT = 100 * 1000;

    private static final int DEFAULT_BUFFER_SIZE = 65536;

    private static final String DEFAULT_CLIENT_NAME = "default_client";

    /**
     * @param brokerHosts host1:9092,host2:9092,host3:9092
     * @param topic topic
     * @return TopicMetadata
     */
    public static TopicMetadata findTopicMetadata(String brokerHosts, String topic) {
        String[] arr = brokerHosts.split(",");
        Preconditions.checkArgument(Objects.nonNull(arr) && arr.length > 0);

        for (String item : arr) {
            HostAndPort hostAndPort = HostAndPort.fromString(item);
            SimpleConsumer consumer = null;

            try {
                consumer = new SimpleConsumer(hostAndPort.getHostText(), hostAndPort.getPort(), DEFAULT_TIMEOUT, DEFAULT_BUFFER_SIZE, DEFAULT_CLIENT_NAME);
                TopicMetadataResponse resp = consumer.send(new TopicMetadataRequest(Collections.singletonList(topic)));
                List<TopicMetadata> topicMetadataList = resp.topicsMetadata();
                if (CollectionUtils.isNotEmpty(topicMetadataList)) {
                    return topicMetadataList.get(0);
                }
            } catch (Exception e) {
                logger.warn("", e);
            } finally {
                KafkaUtils.closeSimpleConsumer(consumer);
            }
        }

        return null;
    }

    public static List<PartitionMetadata> findTopicPartitionMetadata(String brokerHosts, String topic) {
        TopicMetadata topicMetadata = KafkaUtils.findTopicMetadata(brokerHosts, topic);
        if (Objects.nonNull(topicMetadata)) {
            return topicMetadata.partitionsMetadata();
        }

        return Collections.emptyList();
    }

    public static PartitionMetadata findPartitionMetadata(String brokerHosts, String topic, int partition) {
        List<PartitionMetadata> partitionMetadataList = KafkaUtils.findTopicPartitionMetadata(brokerHosts, topic);
        if (CollectionUtils.isNotEmpty(partitionMetadataList)) {
            for (PartitionMetadata metadata : partitionMetadataList) {
                if (metadata.partitionId() == partition) {
                    return metadata;
                }
            }
        }

        return null;
    }

    public static void closeSimpleConsumer(SimpleConsumer consumer) {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                logger.warn("", e);
            }
        }
    }

    public static String getConsumerGroupOwnerZKNode(String groupId, String topic) {
        return "/consumers/" + groupId + "/owners/" + topic;
    }

    public static String getConsumerGroupOffsetZKNode(String groupId, String topic) {
        return "/consumers/" + groupId + "/offsets/" + topic;
    }

    public static boolean isKafkaClusterAlive(String zkList, String brokerNamespace) {
        CuratorFramework zkClient = null;

        try {
            String brokerPath = (StringUtils.isNotEmpty(brokerNamespace) ? "/" + brokerNamespace : "") + "/brokers/ids";
            zkClient = ZKUtils.createCuratorFramework(zkList, null);
            return CollectionUtils.isNotEmpty(ZKUtils.getChildren(zkClient, brokerPath));
        } catch (Exception e) {
            logger.warn("", e);
        } finally {
            ZKUtils.closeCuratorFramework(zkClient);
        }

        return false;
    }
}

