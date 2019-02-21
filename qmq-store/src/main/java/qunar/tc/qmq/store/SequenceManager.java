package qunar.tc.qmq.store;

/**
 * Created by zhaohui.yu
 * 2/20/19
 */
public interface SequenceManager {
    void adjustConsumerLogMinOffset(LogSegment segment);

    long getOffsetOrDefault(final String subject, final long defaultVal);

    long incOffset(final String subject);
}
