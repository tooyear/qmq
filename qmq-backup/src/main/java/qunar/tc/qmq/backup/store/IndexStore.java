package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.backup.model.Index;

import java.util.List;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public interface IndexStore {
    void put(List<Index> index);

    List<Index> scan(byte[] startKey, int limit);
}
