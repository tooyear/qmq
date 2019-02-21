package qunar.tc.qmq.backup.model;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class BackupMessage {
    private final String subject;
    private final byte[] messageId;
    private final long createTime;
    private final long sequence;

    private final long offset;
    private final int size;
    private long baseOffset;

    public BackupMessage(String subject, byte[] messageId, long createTime,
                         long sequence, long offset, int size, long baseOffset) {
        this.subject = subject;
        this.messageId = messageId;
        this.createTime = createTime;
        this.sequence = sequence;
        this.offset = offset;
        this.size = size;
        this.baseOffset = baseOffset;
    }

    public long getOffset() {
        return offset;
    }

    public String getSubject() {
        return subject;
    }

    public byte[] getMessageId() {
        return messageId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getSequence() {
        return sequence;
    }

    public int getSize() {
        return size;
    }

    public long getBaseOffset() {
        return baseOffset;
    }
}
