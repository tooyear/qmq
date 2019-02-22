package qunar.tc.qmq.backup.service.impl;

import qunar.tc.qmq.backup.model.BackupMessage;
import qunar.tc.qmq.backup.model.Index;
import qunar.tc.qmq.backup.model.Page;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.service.IndexService;
import qunar.tc.qmq.backup.store.IndexStore;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class IndexServiceImpl implements IndexService {

    private final DicService dicService;
    private final IndexStore indexStore;

    private final BackupKeyGenerator keyGenerator;

    public IndexServiceImpl(IndexStore indexStore, DicService dicService) {
        this.dicService = dicService;
        this.indexStore = indexStore;
        this.keyGenerator = new BackupKeyGenerator(dicService);
    }

    @Override
    public void put(List<BackupMessage> messages) {
        List<Index> indices = new ArrayList<>(messages.size());
        for (BackupMessage message : messages) {
            byte[] key = null;
            if (RetrySubjectUtils.isRetrySubject(message.getSubject())) {
                key = keyGenerator.generateRetryKey(message);
            } else if (RetrySubjectUtils.isDeadRetrySubject(message.getSubject())) {
                key = keyGenerator.generateKey(message);
                //add action for dead message(sequence = -1)
            } else {
                key = keyGenerator.generateKey(message);
            }
            Index index = new Index(key, message.getMessageId(), message.getSequence(), message.getOffset(), message.getSize());
            indices.add(index);
        }

        indexStore.put(indices);
    }

    @Override
    public Index get(String subject, long createTime, String messageId) {
        return null;
    }

    @Override
    public CompletableFuture<Page> scan(String subject, long startTime, long endTime, byte[] startKey, int limit) {
        if (startKey == null) {
            startKey = keyGenerator.generatePrefixKey(subject, startTime);
        }
        byte[] endKey = keyGenerator.generatePrefixKey(subject, endTime);
        String regex = "^" + dicService.name2Id(subject) + "\\d{12}";
        return indexStore.scan(startKey, endKey, regex, limit);
    }
}
