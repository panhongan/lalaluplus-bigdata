package com.github.panhongan.bigdata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Objects;

/**
 * @author lalaluplus
 * @since 2022.1.20
 */

@Ignore
public class HdfsHelperTest {

    @Test
    public void test() throws Exception {
        NameNodeInfo nameNodeInfo1 = new NameNodeInfo();
        nameNodeInfo1.setNameNodeId("namenode37");
        nameNodeInfo1.setNameNodeHostAndPort("kvm-100-105:8020");

        NameNodeInfo nameNodeInfo2 = new NameNodeInfo();
        nameNodeInfo2.setNameNodeId("namenode24");
        nameNodeInfo2.setNameNodeHostAndPort("kvm-100-149:8020");

        try {
            FileSystem fileSystem = HdfsHelper.getOrCreateFileSystem(nameNodeInfo1);
            if (Objects.nonNull(fileSystem)) {
                for (int i = 0; i < 100; ++i) {
                    System.out.println(fileSystem.exists(new Path("/test_spark/input")));

                    Thread.sleep(5000);
                }
            }
        } finally {
            HdfsHAHelper.closeFileSystem();
        }
    }
}
