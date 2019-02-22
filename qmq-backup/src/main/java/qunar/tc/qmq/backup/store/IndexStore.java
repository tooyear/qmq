package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.backup.model.Index;
import qunar.tc.qmq.backup.model.Page;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public interface IndexStore {
    void put(List<Index> index);

    CompletableFuture<Page> scan(byte[] startKey, byte[] endKey, String regex, int limit);
}
