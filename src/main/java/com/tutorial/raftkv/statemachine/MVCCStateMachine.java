package com.tutorial.raftkv.statemachine;

import com.tutorial.raftkv.mvcc.MVCCStore;
import com.tutorial.raftkv.raft.LogEntry;
import java.util.HashMap;
import java.util.Map;

/**
 * MVCC-based implementation of StateMachine.
 * Wraps our existing MVCCStore to implement the generic interface.
 */
public class MVCCStateMachine implements StateMachine {

    private final MVCCStore mvccStore;

    public MVCCStateMachine() {
        this.mvccStore = new MVCCStore();
    }

    @Override
    public void apply(LogEntry.Command command) {
        switch (command.getType()) {
            case PUT:
                mvccStore.put(command.getKey(), command.getValue());
                break;
            case DELETE:
                mvccStore.delete(command.getKey());
                break;
            case NO_OP:
                // No operation
                break;
        }
    }

    @Override
    public Object read(String key) {
        return mvccStore.get(key);
    }

    @Override
    public Map<String, Object> getSnapshot() {
        Map<String, String> mvccSnapshot = mvccStore.snapshot();
        return new HashMap<>(mvccSnapshot);
    }

    @Override
    public long getCurrentVersion() {
        return mvccStore.getCurrentVersion();
    }

    // MVCC supports transactions
    @Override
    public long beginTransaction() {
        return mvccStore.beginTransaction();
    }

    @Override
    public Object readInTransaction(long txId, String key) {
        return mvccStore.getInTransaction(txId, key);
    }

    @Override
    public void commitTransaction(long txId) {
        mvccStore.commitTransaction(txId);
    }

    @Override
    public Object readAtVersion(String key, long version) {
        return mvccStore.getAtVersion(key, version);
    }

    // Expose the underlying MVCCStore for advanced operations
    public MVCCStore getMvccStore() {
        return mvccStore;
    }
}