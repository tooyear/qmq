package qunar.tc.qmq.backup.startup;

import qunar.tc.qmq.store.LogSegment;
import qunar.tc.qmq.store.SequenceManager;

/**
 * Created by zhaohui.yu
 * 2/20/19
 */
public class NopSequenceManager implements SequenceManager {
    @Override
    public void adjustConsumerLogMinOffset(LogSegment segment) {

    }

    @Override
    public long getOffsetOrDefault(String subject, long defaultVal) {
        return 0;
    }

    @Override
    public long incOffset(String subject) {
        return 0;
    }
}
