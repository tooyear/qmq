package qunar.tc.qmq.backup.store.impl;

import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import qunar.tc.qmq.backup.model.Index;
import qunar.tc.qmq.backup.store.IndexStore;
import qunar.tc.qmq.configuration.DynamicConfig;

import java.util.ArrayList;
import java.util.List;

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

    public HBaseIndexStore(DynamicConfig config, String brokerNameId) {
        final org.hbase.async.Config hbaseConfig = from(config);
        this.client = new HBaseClient(hbaseConfig);
        this.indexTable = Bytes.UTF8("qmq_backup_index");
        this.indexFamily = Bytes.UTF8("i");
        this.indexColumns = Bytes.UTF8("c");
        this.brokerName = Bytes.UTF8(brokerNameId);
    }


    @Override
    public void put(List<Index> batch) {
        List<Deferred<Object>> list = new ArrayList<>(batch.size());
        for (Index index : batch) {
            byte[] value = new byte[2 + index.getMessageId().length + 8 + 8 + 4 + brokerName.length];

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

            System.arraycopy(brokerName, 0, value, writerIndex, brokerName.length);

            PutRequest request = new PutRequest(indexTable, index.getKey(), indexFamily, indexColumns, value);
            Deferred<Object> deferred = client.put(request);
            list.add(deferred);
        }
    }

    @Override
    public List<Index> scan(byte[] startKey, int limit) {
        Scanner scanner = client.newScanner(indexTable);
        scanner.setStartKey(startKey);
        scanner.setMaxNumRows(limit);
        scanner.nextRows();
        return null;
    }
}
