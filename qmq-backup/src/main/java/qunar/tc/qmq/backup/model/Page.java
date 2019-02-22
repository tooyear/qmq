package qunar.tc.qmq.backup.model;

import java.util.Collections;
import java.util.List;

/**
 * Created by zhaohui.yu
 * 2/22/19
 */
public class Page {
    private final byte[] startKey;

    private final byte[] endKey;

    private final List<Index> indices;

    public static final Page EMPTY = new Page(null, null, Collections.emptyList());

    public Page(byte[] startKey, byte[] endKey, List<Index> indices) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.indices = indices;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public List<Index> getIndices() {
        return indices;
    }
}
