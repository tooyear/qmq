package qunar.tc.qmq.backup.startup;

import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.ConsumeTrackService;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.service.OffsetManager;
import qunar.tc.qmq.backup.service.impl.DbDicService;
import qunar.tc.qmq.backup.service.impl.IndexServiceImpl;
import qunar.tc.qmq.backup.store.ActionStore;
import qunar.tc.qmq.backup.store.LocalKVStore;
import qunar.tc.qmq.backup.store.impl.HBaseActionStore;
import qunar.tc.qmq.backup.store.impl.HBaseIndexStore;
import qunar.tc.qmq.backup.store.impl.JdbcDicStore;
import qunar.tc.qmq.backup.store.impl.RocksDBStore;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.meta.BrokerRegisterService;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.store.MessageLog;
import qunar.tc.qmq.store.MessageLogIterateService;
import qunar.tc.qmq.store.PeriodicFlushService;
import qunar.tc.qmq.store.StorageConfigImpl;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;
import qunar.tc.qmq.sync.MasterSlaveSyncManager;
import qunar.tc.qmq.sync.SlaveSyncClient;
import qunar.tc.qmq.sync.SyncType;

import static qunar.tc.qmq.constants.BrokerConstants.*;

/**
 * Created by zhaohui.yu
 * 2/20/19
 */
public class Bootstrap {
    public static void main(String[] args) {
        DynamicConfig config = DynamicConfigLoader.load("backup.properties");
        OffsetManager offsetManager = new OffsetManager(config);
        offsetManager.start();

        Integer listenPort = config.getInt(PORT_CONFIG, DEFAULT_PORT);
        final MetaServerLocator metaServerLocator = new MetaServerLocator(config.getString(META_SERVER_ENDPOINT));
        BrokerRegisterService brokerRegisterService = new BrokerRegisterService(listenPort, metaServerLocator);
        brokerRegisterService.start();

        MasterSlaveSyncManager slaveSyncManager = new MasterSlaveSyncManager(new SlaveSyncClient(config));

        BackupMessageLog messageLog = new BackupMessageLog(new StorageConfigImpl(config), new NopSequenceManager());
        FixedExecOrderEventBus dispatcher = new FixedExecOrderEventBus();
        DicService dicService = new DbDicService(new JdbcDicStore("qmq_dic"));
        String brokerNameId = dicService.name2Id(BrokerConfig.getBrokerName());
        dispatcher.post(new IndexBuilder(config, new IndexServiceImpl(new HBaseIndexStore(config, brokerNameId), dicService), offsetManager));
        MessageLogIterateService iterateService = new MessageLogIterateService(messageLog, offsetManager.getMessageLogIterateOffset(), dispatcher);
        iterateService.start();
        iterateService.blockUntilReplayDone();

        PeriodicFlushService flushService = new PeriodicFlushService(new MessageLogFlusher(messageLog));
        flushService.start();

        slaveSyncManager.registerProcessor(SyncType.message, new MessageSyncProcessor(messageLog));
        LocalKVStore kvStore = new RocksDBStore(config);
        ActionStore actionStore = new HBaseActionStore(config, brokerNameId, dicService, new BackupKeyGenerator(dicService));
        slaveSyncManager.registerProcessor(SyncType.action, new BackupActionLogSyncProcessor(offsetManager, new ConsumeTrackService(kvStore, actionStore)));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            safeClose(flushService);
            safeClose(offsetManager);
        }));

        slaveSyncManager.startSync();
    }

    private static void safeClose(AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception ignore) {
        }
    }

    private static class MessageLogFlusher implements PeriodicFlushService.FlushProvider {
        private final MessageLog messageLog;

        public MessageLogFlusher(MessageLog messageLog) {
            this.messageLog = messageLog;
        }

        @Override
        public int getInterval() {
            return 50;
        }

        @Override
        public void flush() {
            messageLog.flush();
        }
    }
}
