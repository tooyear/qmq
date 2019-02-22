package qunar.tc.qmq.backup.store.impl;

import com.stumbleupon.async.Deferred;
import org.hbase.async.*;
import qunar.tc.qmq.backup.model.Index;
import qunar.tc.qmq.backup.model.Page;
import qunar.tc.qmq.backup.store.IndexStore;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class HBaseIndexStore extends HBaseConfig implements IndexStore {

    private final HBaseClient client;

    private final byte[] indexTable;
    private final byte[] indexFamily;
    private final byte[] indexColumns;

    private final byte[] brokerName;
    private final byte[] brokerNameLen;

    public HBaseIndexStore(DynamicConfig config) {
        final org.hbase.async.Config hbaseConfig = from(config);
        this.client = new HBaseClient(hbaseConfig);
        this.indexTable = Bytes.UTF8("qmq_backup_index_v2");
        this.indexFamily = Bytes.UTF8("i");
        this.indexColumns = Bytes.UTF8("c");
        this.brokerName = Bytes.UTF8(BrokerConfig.getBrokerName());
        this.brokerNameLen = Bytes.fromShort((short) brokerName.length);
    }

    @Override
    public void put(List<Index> batch) {
        System.out.println("receive" + batch.size());
        List<Deferred<Object>> list = new ArrayList<>(batch.size());
        for (Index index : batch) {
            byte[] value = new byte[2 + index.getMessageId().length + 8 + 8 + 4 + 2 + brokerName.length];

            int writerIndex = 0;
            byte[] len = Bytes.fromShort((short) index.getMessageId().length);
            System.arraycopy(len, 0, value, writerIndex, 2);
            writerIndex += 2;

            System.arraycopy(index.getMessageId(), 0, value, writerIndex, index.getMessageId().length);
            writerIndex += index.getMessageId().length;

            System.arraycopy(Bytes.fromLong(index.getSequence()), 0, value, writerIndex, 8);
            writerIndex += 8;

            System.arraycopy(Bytes.fromLong(index.getOffset()), 0, value, writerIndex, 8);
            writerIndex += 8;

            System.arraycopy(Bytes.fromInt(index.getSize()), 0, value, writerIndex, 4);
            writerIndex += 4;

            System.arraycopy(brokerNameLen, 0, value, writerIndex, 2);
            writerIndex += 2;
            System.arraycopy(brokerName, 0, value, writerIndex, brokerName.length);

            PutRequest request = new PutRequest(indexTable, index.getKey(), indexFamily, indexColumns, value);
            Deferred<Object> deferred = client.put(request);
            list.add(deferred);
        }
    }

    @Override
    public CompletableFuture<Page> scan(byte[] startKey, byte[] endKey, String regex, int limit) {
        Scanner scanner = client.newScanner(indexTable);
        scanner.setStartKey(startKey);
        scanner.setStopKey(endKey);
        scanner.setKeyRegexp(regex);
        scanner.setMaxNumRows(limit);
        Deferred<ArrayList<ArrayList<KeyValue>>> rows = scanner.nextRows();
        CompletableFuture<Page> future = new CompletableFuture<>();
        rows.addBoth(arg -> {
            if (arg == null || arg.size() == 0) {
                future.complete(Page.EMPTY);
                return null;
            }

            byte[] firstKey = null;
            byte[] stopKey = null;
            List<Index> result = new ArrayList<>(arg.size());
            for (int i = 0; i < arg.size(); ++i) {
                ArrayList<KeyValue> row = arg.get(i);
                if (row.size() == 0) continue;
                KeyValue cell = row.get(0);
                if (i == 0) {
                    firstKey = cell.key();
                }
                if (i == (arg.size() - 1)) {
                    stopKey = cell.key();
                }
                result.add(decode(cell));
            }
            Page page = new Page(firstKey, stopKey, result);
            future.complete(page);
            return null;
        });
        return future;
    }

    private Index decode(KeyValue cell) {
        byte[] value = cell.value();

        try {
            int readerIndex = 0;
            short messageIdLen = Bytes.getShort(value, readerIndex);
            readerIndex += 2;
            byte[] messageId = new byte[messageIdLen];
            System.arraycopy(value, readerIndex, messageId, 0, messageIdLen);
            readerIndex += messageIdLen;
            long sequence = Bytes.getLong(value, readerIndex);
            readerIndex += 8;
            long offset = Bytes.getLong(value, readerIndex);
            readerIndex += 8;
            int size = Bytes.getInt(value, readerIndex);
            readerIndex += 4;
            short brokerNameLen = Bytes.getShort(value, readerIndex);
            readerIndex += 2;
            String brokerName = new String(value, readerIndex, brokerNameLen, "UTF-8");
            Index index = new Index(cell.key(), messageId, sequence, offset, size);
            index.setBrokerName(brokerName);
            return index;
        } catch (Exception e) {
            return null;
        }
    }
}
