package qunar.tc.qmq.backup.startup;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.store.MessageLog;
import qunar.tc.qmq.sync.AbstractSyncLogProcessor;
import qunar.tc.qmq.sync.SyncType;

/**
 * Created by zhaohui.yu
 * 2/20/19
 */
public class MessageSyncProcessor extends AbstractSyncLogProcessor {
    private final MessageLog messageLog;

    public MessageSyncProcessor(MessageLog messageLog) {
        this.messageLog = messageLog;
    }

    @Override
    public void appendLogs(long startOffset, ByteBuf body) {
        messageLog.appendData(startOffset, body.nioBuffer());
    }

    @Override
    public SyncRequest getRequest() {
        final long messageLogMaxOffset = messageLog.getMaxOffset();
        return new SyncRequest(SyncType.message.getCode(), messageLogMaxOffset, 0L);
    }
}
