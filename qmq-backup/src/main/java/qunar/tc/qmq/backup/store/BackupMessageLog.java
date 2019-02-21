package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.backup.model.BackupMessage;
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class BackupMessageLog extends MessageLog {
    public BackupMessageLog(StorageConfig config, SequenceManager sequenceManager) {
        super(config, sequenceManager);
    }

    public MessageLogVisitor newVisitor(long iterateFrom) {
        return new BackupMessageLogVisitor(logManager, iterateFrom);
    }

    private static class BackupMessageLogVisitor extends MessageLogVisitor<BackupMessage> {

        public BackupMessageLogVisitor(LogManager logManager, long startOffset) {
            super(logManager, startOffset);
        }

        @Override
        protected BackupMessage createRecord(String subject, long sequence, long wroteOffset, int wroteBytes, short headerSize, long baseOffset, ByteBuffer payload) {
            payload.get();
            long createTime = payload.getLong();
            payload.position(payload.position() + 8);
            PayloadHolderUtils.readString(payload);
            short messageIdLen = payload.getShort();
            byte[] messageId = new byte[messageIdLen];
            payload.get(messageId);
            return new BackupMessage(subject, messageId, createTime, sequence, wroteOffset, wroteBytes, baseOffset);
        }
    }

    public LogSegment getFirstInActiveSegment() {
        LogSegment first = logManager.firstSegment();
        LogSegment latest = logManager.latestSegment();
        if (first == latest) return null;
        return first;
    }

    public void delete(LogSegment logSegment) {
        logManager.deleteSegmentsBeforeOffset(logSegment.getBaseOffset() + 1);
    }
}
