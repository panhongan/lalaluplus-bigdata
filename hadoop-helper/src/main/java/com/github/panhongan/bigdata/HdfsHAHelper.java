package com.github.panhongan.bigdata;

import java.util.Objects;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @author lalaluplus
 * @since 2022.1.20
 */

@Slf4j
public class HdfsHAHelper {

    private static FileSystem fileSystem;

    synchronized public static FileSystem getOrCreateFileSystem(HdfsClusterInfo hdfsClusterInfo) {
        if (Objects.isNull(fileSystem)) {
            String nameService = hdfsClusterInfo.getNameService();

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://" + nameService);
            conf.set("dfs.nameservices", nameService);
            conf.set("dfs.ha.namenodes." + nameService,
                    hdfsClusterInfo.getNameNodeInfo1().getNameNodeId() + "," + hdfsClusterInfo.getNameNodeInfo2().getNameNodeId());
            conf.set(String.format("dfs.namenode.rpc-address.%s.%s", nameService, hdfsClusterInfo.getNameNodeInfo1().getNameNodeId()),
                    hdfsClusterInfo.getNameNodeInfo1().getNameNodeHostAndPort());
            conf.set(String.format("dfs.namenode.rpc-address.%s.%s", nameService, hdfsClusterInfo.getNameNodeInfo2().getNameNodeId()),
                    hdfsClusterInfo.getNameNodeInfo2().getNameNodeHostAndPort());
            conf.set("dfs.client.failover.proxy.provider." + nameService,
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

            try {
                fileSystem = FileSystem.get(conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return fileSystem;
    }

    public static void closeFileSystem() {
        if (Objects.nonNull(fileSystem)) {
            try {
                fileSystem.close();
                fileSystem = null;
            } catch (Exception e) {
                log.warn("", e);
            }
        }
    }

    public static String readTextFileOnce(HdfsClusterInfo hdfsClusterInfo, String filePath) throws Exception {
        FileSystem fileSystem;

        try {
            fileSystem = HdfsHAHelper.getOrCreateFileSystem(hdfsClusterInfo);
            Preconditions.checkNotNull(fileSystem);

            byte[] bytes = IOUtils.readFullyToByteArray(fileSystem.open(new Path(filePath)));
            return new String(bytes);
        } finally {
            HdfsHAHelper.closeFileSystem();
        }
    }
}
