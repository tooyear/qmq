package qunar.tc.qmq.backup.service;

import com.google.common.base.Charsets;
import com.google.common.io.LineReader;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.*;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class OffsetManager implements AutoCloseable {
    private static final int VERSION = 1;
    private static final char NEWLINE = '\n';
    private long messageLogIterateOffset;
    private long actionSyncPoint;
    private final CheckpointStore<Offset> store;
    private final StorageConfig storageConfig;
    private final PeriodicFlushService flushService;

    public OffsetManager(DynamicConfig config) {
        storageConfig = new StorageConfigImpl(config);
        store = new CheckpointStore<Offset>(storageConfig.getCheckpointStorePath(), "checkpoint", new OffsetCheckPointSerde());
        flushService = new PeriodicFlushService(new PeriodicFlushService.FlushProvider() {
            @Override
            public int getInterval() {
                return (int) storageConfig.getMessageCheckpointInterval();
            }

            @Override
            public void flush() {
                store.saveCheckpoint(new Offset(messageLogIterateOffset, actionSyncPoint));
            }
        });
    }

    public void start() {
        Offset offset = this.store.loadCheckpoint();
        messageLogIterateOffset = offset.messageLogIterateOffset;
        actionSyncPoint = offset.actionSyncPoint;

        flushService.start();
    }

    public void setMessageLogIterateOffset(long messageLogIterateOffset) {
        this.messageLogIterateOffset = messageLogIterateOffset;
    }

    public void setActionSyncPoint(long actionSyncPoint) {
        this.actionSyncPoint = actionSyncPoint;
    }

    public long getMessageLogIterateOffset() {
        return this.messageLogIterateOffset;
    }

    public long getActionSyncPoint() {
        return this.actionSyncPoint;
    }


    @Override
    public void close() throws Exception {
        flushService.close();
    }

    private static class Offset {
        public final long messageLogIterateOffset;
        public final long actionSyncPoint;

        private Offset(long messageLogIterateOffset, long actionSyncPoint) {
            this.messageLogIterateOffset = messageLogIterateOffset;
            this.actionSyncPoint = actionSyncPoint;
        }
    }

    private static class OffsetCheckPointSerde implements Serde<Offset> {

        @Override
        public byte[] toBytes(Offset value) {
            String result = String.valueOf(VERSION) + NEWLINE +
                    value.messageLogIterateOffset + NEWLINE +
                    value.actionSyncPoint + NEWLINE;
            return result.getBytes(Charsets.UTF_8);
        }

        @Override
        public Offset fromBytes(byte[] data) {
            final LineReader reader = new LineReader(new StringReader(new String(data, Charsets.UTF_8)));
            try {
                int version = Integer.parseInt(reader.readLine());
                if (VERSION != version) return null;
                Long messageLogIterateOffset = Long.valueOf(reader.readLine());
                Long actionSyncPoint = Long.valueOf(reader.readLine());
                return new Offset(messageLogIterateOffset, actionSyncPoint);
            } catch (IOException e) {
                return null;
            }
        }
    }
}
