package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.store.action.PullAction;
import qunar.tc.qmq.store.action.RangeAckAction;

import java.util.List;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public interface ActionStore {
    void put(PullAction action);

    void put(RangeAckAction ackAction, List<String> consumeSequences);
}
