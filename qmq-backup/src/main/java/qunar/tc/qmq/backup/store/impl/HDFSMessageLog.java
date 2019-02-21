package qunar.tc.qmq.backup.store.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.store.DFSMessageLog;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.LogSegment;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class HDFSMessageLog implements DFSMessageLog {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSMessageLog.class);
    private final FileSystem fileSystem;

    private final String remoteDir;

    private final String brokerName;

    public HDFSMessageLog(DynamicConfig config) {
        fileSystem = createFileSystem(config);
        remoteDir = config.getString("qmq.message.dfs.dir", "/qmq/message");
        brokerName = BrokerConfig.getBrokerName();
    }

    private FileSystem createFileSystem(DynamicConfig config) {
        FileSystem fs;
        URI hdfsUri;
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", false);
        try {
            String uri = config.getString("hdfs.uri");
            hdfsUri = new URI(uri);
            fs = FileSystem.get(hdfsUri, conf);
        } catch (Exception e) {
            LOG.error("HDFS uploader init file-system error.", e);
            throw new RuntimeException("create hdfs failed", e);
        }

        return fs;
    }

    @Override
    public void transferTo(LogSegment logSegment) {
        try {
            FSDataOutputStream os = fileSystem.create(new Path(remoteDir, brokerName + "-" + logSegment.getBaseOffset()), true);
            ByteBuffer srcBuffer = logSegment.sliceByteBuffer();
            byte[] buffer = new byte[1024];
            while (srcBuffer.hasRemaining()) {
                int len = Math.min(1024, srcBuffer.remaining());
                srcBuffer.get(buffer, 0, len);
                os.write(buffer, 0, len);
            }
            os.flush();
        } catch (IOException e) {
            throw new RuntimeException("write hdfs failed");
        }
    }
}
