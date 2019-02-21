package qunar.tc.qmq.backup.service.impl;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.store.DicStore;

import java.util.concurrent.TimeUnit;

/**
 * @author yiqun.fan create on 17-10-31.
 */
public class DbDicService implements DicService {
    private static final int MAX_SIZE = 1000000;
    private static final String ID_FORMAT_PATTERN = "%05d";

    private final DicStore dicStore;
    private final LoadingCache<String, String> name2IdCache;

    public DbDicService(DicStore dicStore) {
        this.dicStore = dicStore;
        this.name2IdCache = CacheBuilder.newBuilder()
                .maximumSize(MAX_SIZE).expireAfterAccess(1, TimeUnit.DAYS)
                .build(new CacheLoader<String, String>() {
                    public String load(String key) {
                        try {
                            return getOrCreateId(key);
                        } catch (EmptyResultDataAccessException e) {
                            return "";
                        }
                    }
                });
    }

    @Override
    public String name2Id(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return "";
        }
        return name2IdCache.getUnchecked(name);
    }

    private String getOrCreateId(String name) {
        int id;
        try {
            id = dicStore.getId(name);
        } catch (EmptyResultDataAccessException e) {
            try {
                id = dicStore.insertName(name);
            } catch (DuplicateKeyException e2) {
                id = dicStore.getId(name);
            }
        }
        return String.format(ID_FORMAT_PATTERN, id);
    }
}
