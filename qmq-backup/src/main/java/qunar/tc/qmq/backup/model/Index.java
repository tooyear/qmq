package qunar.tc.qmq.backup.model;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class Index {
    private final byte[] key;

    private final byte[] messageId;

    private final long sequence;

    private final long offset;

    private final int size;

    private String brokerName;

    public Index(byte[] key, byte[] messageId, long sequence, long offset, int size) {
        this.key = key;
        this.messageId = messageId;
        this.sequence = sequence;
        this.offset = offset;
        this.size = size;
    }

    public byte[] getKey() {
        return key;
    }

    public long getSequence() {
        return sequence;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public byte[] getMessageId() {
        return messageId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
