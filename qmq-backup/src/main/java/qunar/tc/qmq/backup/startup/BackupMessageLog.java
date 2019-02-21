package qunar.tc.qmq.backup.startup;

import qunar.tc.qmq.backup.model.BackupMessage;
import qunar.tc.qmq.store.*;

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
        protected BackupMessage createRecord(String subject, long sequence, long wroteOffset, int wroteBytes, short headerSize, long baseOffset, ByteBuffer currentBuffer) {

            return new BackupMessage(subject, null, -1, sequence, wroteOffset, wroteBytes, baseOffset);
        }
    }
}
