package qunar.tc.qmq.backup.service;

import qunar.tc.qmq.backup.model.Index;
import qunar.tc.qmq.backup.model.BackupMessage;

import java.util.List;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public interface IndexService {
    void put(List<BackupMessage> messages);

    Index get(String subject, long createTime, String messageId);

    List<Index> scan(String subject, long startTime, int limit);
}
