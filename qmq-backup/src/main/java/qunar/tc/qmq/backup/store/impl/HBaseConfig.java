package qunar.tc.qmq.backup.store.impl;

import org.hbase.async.Config;
import qunar.tc.qmq.configuration.DynamicConfig;

import java.util.Map;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
class HBaseConfig {
    Config from(DynamicConfig config) {
        Map<String, String> map = config.asMap();
        final org.hbase.async.Config hbaseConfig = new org.hbase.async.Config();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            hbaseConfig.overrideConfig(entry.getKey(), entry.getValue());
        }
        return hbaseConfig;
    }
}
