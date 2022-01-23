package com.github.panhongan.bigdata.kakfa;

import com.github.panhongan.bigdata.kafka.KafkaUtils;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

/**
 * @author lalaluplus
 * @since 2021.11.10
 */

@Ignore
public class KafkaUtilsTest {

    private static final String BROKER_LIST = "192.168.100.105:9092,192.168.100.232:9092,192.168.100.222:9092";

    private static final String ZK_LIST = "192.168.100.105:2181,192.168.100.232:2181,192.168.100.222:2181";

    private static final String TOPIC = "reject";

    @Test
    public void testFindTopicMetadata() {
        TopicMetadata topicMetadata = KafkaUtils.findTopicMetadata(BROKER_LIST, TOPIC);
        assert (topicMetadata != null);
        assert (Objects.equals(topicMetadata.topic(), TOPIC));
    }

    @Test
    public void testFindTopicPartitionMetadata() {
        List<PartitionMetadata> partitionMetadataList = KafkaUtils.findTopicPartitionMetadata(BROKER_LIST, TOPIC);
        assert (CollectionUtils.isNotEmpty(partitionMetadataList));
    }

    @Test
    public void testFindPartitionMetadata() {
        PartitionMetadata partitionMetadata = KafkaUtils.findPartitionMetadata(BROKER_LIST, TOPIC, 0);
        assert (Objects.nonNull(partitionMetadata));
    }

    @Test
    public void testIsKafkaAlive() {
        boolean isAlive = KafkaUtils.isKafkaClusterAlive(ZK_LIST, null);
        assert (isAlive);
    }
}
