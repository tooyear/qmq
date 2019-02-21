package qunar.tc.qmq.store;

import java.nio.ByteBuffer;

/**
 * Created by zhaohui.yu
 * 2/20/19
 */
public class MessageLogMetaVisitor extends MessageLogVisitor<MessageLogMeta> {
    public MessageLogMetaVisitor(LogManager logManager, long startOffset) {
        super(logManager, startOffset);
    }

    @Override
    protected MessageLogMeta createRecord(String subject, long sequence, long wroteOffset, int wroteBytes, short headerSize, long baseOffset, ByteBuffer currentBuffer) {
        return new MessageLogMeta(subject, sequence, wroteOffset, wroteBytes, headerSize, baseOffset);
    }
}
