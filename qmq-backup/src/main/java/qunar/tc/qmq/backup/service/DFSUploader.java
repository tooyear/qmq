package qunar.tc.qmq.backup.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.store.BackupMessageLog;
import qunar.tc.qmq.backup.store.DFSMessageLog;
import qunar.tc.qmq.store.LogSegment;

import java.util.concurrent.TimeUnit;

/**
 * Created by zhaohui.yu
 * 2/22/19
 */
public class DFSUploader {
    private static final Logger LOG = LoggerFactory.getLogger(DFSUploader.class);

    private final BackupMessageLog localMessageLog;
    private final DFSMessageLog dfsMessageLog;

    public DFSUploader(BackupMessageLog localMessageLog, DFSMessageLog dfsMessageLog) {
        this.localMessageLog = localMessageLog;
        this.dfsMessageLog = dfsMessageLog;
    }

    public void start() {
        new Thread(() -> {
            while (true) {
                LogSegment segment = localMessageLog.getFirstInActiveSegment();
                if (segment == null) {
                    try {
                        TimeUnit.SECONDS.wait(1);
                    } catch (InterruptedException e) {
                        break;
                    }
                    continue;
                }

                try {
                    dfsMessageLog.transferTo(segment);

                    localMessageLog.delete(segment);
                } catch (Exception e) {
                    LOG.error("write dfs failed", e);
                }
            }
        }, "qmq-dfsuploader").start();
    }
}
