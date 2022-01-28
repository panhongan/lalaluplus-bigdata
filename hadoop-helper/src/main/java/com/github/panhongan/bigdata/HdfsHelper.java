package com.github.panhongan.bigdata;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;
import java.util.Objects;

/**
 * @author lalaluplus
 * @since 2022.1.20
 */

@Slf4j
public class HdfsHelper {

    private static FileSystem fileSystem;

    synchronized public static FileSystem getOrCreateFileSystem(NameNodeInfo nameNodeInfo) {
        if (Objects.isNull(fileSystem)) {

            Configuration conf = new Configuration();

            try {
                URI uri = new URI("hdfs://" + nameNodeInfo.getNameNodeHostAndPort());
                fileSystem = FileSystem.get(uri, conf);
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

    public static String readTextFileOnce(NameNodeInfo nameNodeInfo, String filePath) throws Exception {
        FileSystem fileSystem;

        try {
            fileSystem = HdfsHelper.getOrCreateFileSystem(nameNodeInfo);
            Preconditions.checkNotNull(fileSystem);

            byte[] bytes = IOUtils.readFullyToByteArray(fileSystem.open(new Path(filePath)));
            return new String(bytes);
        } finally {
            HdfsHelper.closeFileSystem();
        }
    }
}
