package qunar.tc.qmq.backup.startup;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.backup.service.ConsumeTrackService;
import qunar.tc.qmq.backup.service.OffsetManager;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.ActionLog;
import qunar.tc.qmq.store.ActionType;
import qunar.tc.qmq.store.MagicCode;
import qunar.tc.qmq.sync.AbstractSyncLogProcessor;
import qunar.tc.qmq.sync.SyncType;

/**
 * Created by zhaohui.yu
 * 4/24/18
 */
public class BackupActionLogSyncProcessor extends AbstractSyncLogProcessor {
    /**
     * 4 bytes magic + 1 byte record type
     */
    private static final int MIN_RECORD_BYTES = 5;

    private final OffsetManager offsetManager;
    private final ConsumeTrackService trackService;

    public BackupActionLogSyncProcessor(OffsetManager offsetManager, ConsumeTrackService trackService) {
        this.offsetManager = offsetManager;
        this.trackService = trackService;
    }

    @Override
    public void appendLogs(long startOffset, ByteBuf input) {
        offsetManager.setActionSyncPoint(startOffset);
        while (input.isReadable(MIN_RECORD_BYTES)) {
            int read = appendLog(offsetManager.getActionSyncPoint(), input);
            if (read > 0) {
            } else {
                break;
            }
        }
    }

    private int appendLog(long currentOffset, ByteBuf buffer) {
        int start = buffer.readerIndex();
        final int magic = buffer.readInt();
        if (magic != MagicCode.ACTION_LOG_MAGIC_V1) {
            return buffer.readerIndex() - start;
        }
        final byte attributes = buffer.readByte();
        if (attributes == 1) {
            buffer.readerIndex(buffer.readerIndex() + buffer.readableBytes());
            long relativePosition = currentOffset % ActionLog.PER_SEGMENT_FILE_SIZE;
            return (int) (ActionLog.PER_SEGMENT_FILE_SIZE - relativePosition);
        } else if (attributes == 2) {
            if (buffer.readableBytes() < Integer.BYTES) {
                return 0;
            }
            final int blankSize = buffer.readInt();
            if (buffer.readableBytes() < blankSize) {
                return 0;
            }
            buffer.readerIndex(buffer.readerIndex() + blankSize);
        } else {
            if (buffer.readableBytes() < Byte.BYTES + Integer.BYTES) {
                return 0;
            }
            final ActionType payloadType = ActionType.fromCode(buffer.readByte());
            final int payloadSize = buffer.readInt();
            if (buffer.readableBytes() < payloadSize) {
                return 0;
            }
            if (buffer.nioBufferCount() > 0) {
                final Action action = payloadType.getReaderWriter().read(buffer.nioBuffer());
                buffer.readerIndex(buffer.readerIndex() + payloadSize);
                if (!processAction(action)) {
                    return 0;
                }
            } else {
                return buffer.readerIndex() - start;
            }
        }
        return buffer.readerIndex() - start;
    }

    private boolean processAction(Action action) {
        return trackService.track(action);
    }


    @Override
    public SyncRequest getRequest() {
        return new SyncRequest(SyncType.action.getCode(), 0, offsetManager.getActionSyncPoint());
    }
}
