package qunar.tc.qmq.backup.store.impl;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.TtlDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.store.LocalKVStore;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.utils.CharsetUtils;

import java.util.Optional;

/**
 * @author yunfeng.yang
 * @since 2018/3/21
 */
public class RocksDBStore implements LocalKVStore {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStore.class);

    static {
        RocksDB.loadLibrary();
    }

    private final TtlDB rocksDB;

    public RocksDBStore(final DynamicConfig config) {
        final String path = config.getString("rocks.db.path");
        final int ttl = config.getInt("rocks.db.ttl");
        try {
            final Options options = new Options();
            options.setCreateIfMissing(true);
            this.rocksDB = TtlDB.open(options, path, ttl, false);
            LOG.info("open rocks db success, path:{}, ttl:{}", path, ttl);
        } catch (Exception e) {
            LOG.error("open rocks db error, path:{}, ttl:{}", path, ttl, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(String key, String value) {
        try {
            final byte[] keyBytes = CharsetUtils.toUTF8Bytes(key);
            final byte[] valueBytes = CharsetUtils.toUTF8Bytes(value);
            if (keyBytes == null || keyBytes.length == 0 || valueBytes == null || value.length() == 0) {
                return;
            }
            rocksDB.put(keyBytes, valueBytes);
        } catch (Exception e) {
            LOG.error("put rocks db error, key:{}, value:{}", key, value, e);
        }
    }

    @Override
    public Optional<String> get(String key) {
        try {
            final byte[] keyBytes = CharsetUtils.toUTF8Bytes(key);
            if (keyBytes == null || keyBytes.length == 0) {
                return Optional.empty();
            }
            final byte[] valueBytes = rocksDB.get(keyBytes);
            final String value = CharsetUtils.toUTF8String(valueBytes);
            if (value.length() == 0) {
                return Optional.empty();
            }
            return Optional.of(value);
        } catch (Exception e) {
            LOG.error("get value from rocks db error, key:{}", key, e);
            return Optional.empty();
        }
    }
}
