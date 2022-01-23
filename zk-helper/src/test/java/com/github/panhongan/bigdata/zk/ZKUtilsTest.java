package com.github.panhongan.bigdata.zk;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * @author lalaluplus
 * @since 2022.1.10
 */

@Ignore
public class ZKUtilsTest {

    private static final String ZK_LIST = "192.168.100.105:2181,192.168.100.232:2181,192.168.100.222:2181";

    private static final String NAME_SPACE = "test";

    @Test
    public void testCreateCuratorFramework() {
        CuratorFramework client = null;

        try {
            client = ZKUtils.createCuratorFramework(ZK_LIST, NAME_SPACE);
            assert (client != null);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ZKUtils.closeCuratorFramework(client);
        }
    }

    @Test
    public void testCreatePersistNode() {
        CuratorFramework client = null;

        try {
            client = ZKUtils.createCuratorFramework(ZK_LIST, NAME_SPACE);
            assert (client != null);

            ZKUtils.createPersistNode(client, "/hello", "hello".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ZKUtils.closeCuratorFramework(client);
        }
    }

    @Test
    public void testCreatePersistNodeWithoutNamespace() {
        CuratorFramework client = null;

        try {
            client = ZKUtils.createCuratorFramework(ZK_LIST, null);
            assert (client != null);

            ZKUtils.createPersistNode(client, "/test1/hello", "hello".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ZKUtils.closeCuratorFramework(client);
        }
    }

    @Test
    public void testCreatePersistSequentialNode() {
        CuratorFramework client = null;

        try {
            client = ZKUtils.createCuratorFramework(ZK_LIST, NAME_SPACE);
            assert (client != null);

            ZKUtils.createPersistSequentialNode(client, "/persist/seq", "hello".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ZKUtils.closeCuratorFramework(client);
        }
    }

    @Test
    public void testCreateEphemeralNode() {
        CuratorFramework client = null;

        try {
            client = ZKUtils.createCuratorFramework(ZK_LIST, NAME_SPACE);
            assert (client != null);

            ZKUtils.createEphemeralNode(client, "/hello-eph", "hello".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ZKUtils.closeCuratorFramework(client);
        }
    }

    @Test
    public void testSetNodeData() {
        CuratorFramework client = null;

        try {
            client = ZKUtils.createCuratorFramework(ZK_LIST, NAME_SPACE);
            assert (client != null);

            ZKUtils.setNodeData(client, "/hello", "hello1".getBytes(StandardCharsets.UTF_8));

            String data = ZKUtils.getNodeData(client, "/hello");
            assert (Objects.equals(data, "hello1"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ZKUtils.closeCuratorFramework(client);
        }
    }

    @Test
    public void testCheckExists() {
        CuratorFramework client = null;

        try {
            client = ZKUtils.createCuratorFramework(ZK_LIST, NAME_SPACE);
            assert (client != null);

            assert (ZKUtils.checkExists(client, "/hello"));
            assert (!ZKUtils.checkExists(client, "/hello2"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ZKUtils.closeCuratorFramework(client);
        }
    }

    @Test
    public void testGetChildren() {
        CuratorFramework client = null;

        try {
            client = ZKUtils.createCuratorFramework(ZK_LIST, NAME_SPACE);
            assert (client != null);

            List<String> children = ZKUtils.getChildren(client, "/");
            assert (CollectionUtils.isNotEmpty(children));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ZKUtils.closeCuratorFramework(client);
        }
    }
}
