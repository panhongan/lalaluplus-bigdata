package com.github.panhongan.bigdata.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.Objects;

/**
 * @author lalaluplus
 * @since 2021.1.10
 */
public class ZKUtils {

    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 30 * 1000;

    private static final int DEFAULT_SESSION_TIMEOUT_MS = 30 * 1000;

    private static final int DEFAULT_SLEEP_MS = 1000;

    private static final int DEFAULT_RETRY_NUM = 3;

    /**
     * @param zkList : host1:2181,host2:2181,host3:2181
     * @param namespace : all nodes under this namespace
     * @return CuratorFramework
     */
    public static CuratorFramework createCuratorFramework(String zkList, String namespace) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zkList)
                .retryPolicy(new ExponentialBackoffRetry(DEFAULT_SLEEP_MS, DEFAULT_RETRY_NUM))
                .connectionTimeoutMs(DEFAULT_CONNECTION_TIMEOUT_MS)
                .sessionTimeoutMs(DEFAULT_SESSION_TIMEOUT_MS)
                .namespace(namespace)
                .build();
        curatorFramework.start();
        return curatorFramework;
    }

    public static void closeCuratorFramework(final CuratorFramework client) {
        if (Objects.nonNull(client)) {
            client.close();
        }
    }

    public static void createPersistNode(final CuratorFramework client, final String path, final byte[] payload) throws Exception {
        client.create().creatingParentsIfNeeded().forPath(path, payload);
    }

    public static String createPersistSequentialNode(final CuratorFramework client, final String path, final byte[] payload) throws Exception {
        return client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, payload);
    }

    public static void createEphemeralNode(final CuratorFramework client, final String path, final byte[] payload) throws Exception {
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
    }

    public static String createEphemeralSequentialNode(final CuratorFramework client, final String path, final byte[] payload) throws Exception {
        return client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, payload);
    }

    public static void setNodeData(final CuratorFramework client, final String path, final byte[] payload) throws Exception {
        client.setData().forPath(path, payload);
    }

    public static void deleteNode(final CuratorFramework client, final String path) throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public static void guaranteedDeleteNode(final CuratorFramework client, final String path) throws Exception {
        client.delete().guaranteed().forPath(path);
    }

    public static String getNodeData(final CuratorFramework client, final String path) throws Exception {
        return new String(client.getData().forPath(path));
    }

    public static Boolean checkExists(final CuratorFramework client, final String path) throws Exception {
        return client.checkExists().forPath(path) != null;
    }

    public static List<String> getChildren(final CuratorFramework client, final String path) throws Exception {
        return client.getChildren().forPath(path);
    }
}
