package qunar.tc.qmq.backup.service;

import qunar.tc.qmq.configuration.DynamicConfig;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class OffsetManager implements AutoCloseable {
    private long messageLogIterateOffset;
    private long actionSyncPoint;

    public OffsetManager(DynamicConfig config) {

    }

    public void start() {

    }

    public void setMessageLogIterateOffset(long messageLogIterateOffset) {
        this.messageLogIterateOffset = messageLogIterateOffset;
    }

    public void setActionSyncPoint(long actionSyncPoint) {
        this.actionSyncPoint = actionSyncPoint;
    }

    public long getMessageLogIterateOffset() {
        return this.messageLogIterateOffset;
    }

    public long getActionSyncPoint() {
        return this.actionSyncPoint;
    }


    @Override
    public void close() throws Exception {

    }
}
