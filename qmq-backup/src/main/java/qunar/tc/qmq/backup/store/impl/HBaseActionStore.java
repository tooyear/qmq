package qunar.tc.qmq.backup.store.impl;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.store.ActionStore;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.action.PullAction;
import qunar.tc.qmq.store.action.RangeAckAction;

import java.util.List;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class HBaseActionStore extends HBaseConfig implements ActionStore {
    private final HBaseClient client;
    private final byte[] trackTable;
    private final byte[] family;
    private final byte[] brokerName;
    private final DicService dicService;
    private final BackupKeyGenerator keyGenerator;

    public HBaseActionStore(DynamicConfig config, String brokerNameId, DicService dicService, BackupKeyGenerator keyGenerator) {
        this.dicService = dicService;
        this.keyGenerator = keyGenerator;
        final org.hbase.async.Config hbaseConfig = from(config);
        this.client = new HBaseClient(hbaseConfig);
        this.trackTable = Bytes.UTF8("qmq_consume_track");
        this.family = Bytes.UTF8("i");
        this.brokerName = Bytes.UTF8(brokerNameId);
    }

    @Override
    public void put(PullAction action) {
        byte[] subjectId = Bytes.UTF8(dicService.name2Id(action.subject()));
        byte[] column = Bytes.UTF8(dicService.name2Id(action.group()) + ":p");
        byte[] consumerId = Bytes.UTF8(action.consumerId());
        for (long sequence = action.getFirstMessageSequence(); sequence < action.getLastMessageSequence(); sequence++) {
            byte[][] parts = new byte[][]{subjectId, this.brokerName, Bytes.fromLong(sequence)};
            byte[] key = keyGenerator.generateRowKey(parts);
            PutRequest request = new PutRequest(trackTable, key, family, column, consumerId);
            request.setDurable(false);
            client.put(request);
        }
    }

    @Override
    public void put(RangeAckAction action, List<String> consumeSequences) {
        byte[] subjectId = Bytes.UTF8(dicService.name2Id(action.subject()));
        byte[] column = Bytes.UTF8(dicService.name2Id(action.group()) + ":a");
        byte[] consumerId = Bytes.UTF8(action.consumerId());
        for (String sequence : consumeSequences) {
            byte[][] parts = new byte[][]{subjectId, this.brokerName, Bytes.UTF8(sequence)};
            byte[] key = keyGenerator.generateRowKey(parts);
            PutRequest request = new PutRequest(trackTable, key, family, column, consumerId);
            request.setDurable(false);
            client.put(request);
        }
    }

}
