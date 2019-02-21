package qunar.tc.qmq.backup.startup;

import qunar.tc.qmq.backup.model.BackupMessage;
import qunar.tc.qmq.backup.service.IndexService;
import qunar.tc.qmq.backup.service.OffsetManager;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class IndexBuilder implements FixedExecOrderEventBus.Listener<BackupMessage> {

    private List<BackupMessage> batch;
    private final DynamicConfig config;
    private final IndexService indexService;
    private final OffsetManager offsetManager;

    public IndexBuilder(DynamicConfig config, IndexService indexService, OffsetManager offsetManager) {
        this.config = config;
        this.indexService = indexService;
        this.offsetManager = offsetManager;
        this.batch = new ArrayList<>();
    }

    @Override
    public void onEvent(BackupMessage event) {
        batch.add(event);
        if (batch.size() > config.getInt("index.batch", 100)) {
            List<BackupMessage> thisBatch = batch;
            batch = new ArrayList<>();

            batchStore(thisBatch);
            offsetManager.setMessageLogIterateOffset(event.getOffset());
        }
    }

    private void batchStore(List<BackupMessage> batch) {
        indexService.put(batch);
    }
}
