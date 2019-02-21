package qunar.tc.qmq.backup.store;

import java.util.Optional;

/**
 * @author yunfeng.yang
 * @since 2018/3/21
 */
public interface LocalKVStore {
    void put(String key, String value);

    Optional<String> get(String key);
}
