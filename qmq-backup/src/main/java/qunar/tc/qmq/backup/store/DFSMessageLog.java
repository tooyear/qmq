package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.store.LogSegment;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public interface DFSMessageLog {



    void transferTo(LogSegment logSegment);
}
