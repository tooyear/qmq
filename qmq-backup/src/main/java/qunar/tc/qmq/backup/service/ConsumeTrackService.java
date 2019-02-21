package qunar.tc.qmq.backup.service;

import qunar.tc.qmq.backup.store.ActionStore;
import qunar.tc.qmq.backup.store.LocalKVStore;
import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.action.PullAction;
import qunar.tc.qmq.store.action.RangeAckAction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by zhaohui.yu
 * 2/21/19
 */
public class ConsumeTrackService {
    private final LocalKVStore kvStore;
    private final ActionStore actionStore;

    public ConsumeTrackService(LocalKVStore kvStore, ActionStore actionStore) {
        this.kvStore = kvStore;
        this.actionStore = actionStore;
    }

    public boolean track(Action action) {
        if (action instanceof PullAction) {
            PullAction pullAction = (PullAction) action;
            if (pullAction.getFirstMessageSequence() > pullAction.getLastMessageSequence()) {
                return true;
            }

            storePullAction(pullAction);
            actionStore.put(pullAction);
        } else if (action instanceof RangeAckAction) {
            RangeAckAction ackAction = (RangeAckAction) action;
            List<String> list = mapMessageSequence(ackAction);
            actionStore.put(ackAction, list);
        }

        return true;
    }

    private void storePullAction(PullAction pullAction) {
        String keyPrefix = pullAction.subject() + "$" + pullAction.group() + "$" + pullAction.consumerId() + "$";
        int inc = 0;
        for (long sequence = pullAction.getFirstSequence(); sequence < pullAction.getLastSequence(); sequence++) {
            String key = keyPrefix + sequence;
            kvStore.put(key, String.valueOf(pullAction.getFirstMessageSequence() + (inc++)));
        }
    }

    private List<String> mapMessageSequence(RangeAckAction ackAction) {
        List<String> result = new ArrayList<>();
        String keyPrefix = ackAction.subject() + "$" + ackAction.group() + "$" + ackAction.consumerId() + "$";
        for (long sequence = ackAction.getFirstSequence(); sequence < ackAction.getLastSequence(); sequence++) {
            String key = keyPrefix + sequence;
            Optional<String> value = kvStore.get(key);
            value.ifPresent(result::add);
        }

        return result;
    }


}
