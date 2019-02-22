package qunar.tc.qmq.backup.service;

import qunar.tc.qmq.backup.model.Index;
import qunar.tc.qmq.backup.model.BackupMessage;
import qunar.tc.qmq.backup.model.Page;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public interface IndexService {
    void put(List<BackupMessage> messages);

    Index get(String subject, long createTime, String messageId);

    CompletableFuture<Page> scan(String subject, long startTime, long endTime, byte[] startKey, int limit);
}
