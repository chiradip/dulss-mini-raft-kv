package com.tutorial.raftkv.statemachine;

import com.tutorial.raftkv.raft.LogEntry;
import java.io.Serializable;
import java.util.Map;

/**
 * Generic State Machine interface for Raft.
 * Any state machine can be replicated by implementing this interface.
 */
public interface StateMachine {

    /**
     * Apply a command to the state machine.
     * This must be deterministic - same commands in same order produce same state.
     */
    void apply(LogEntry.Command command);

    /**
     * Read from the state machine.
     * This should not modify state.
     */
    Object read(String key);

    /**
     * Get a snapshot of the entire state (for debugging/monitoring).
     */
    Map<String, Object> getSnapshot();

    /**
     * Get the current version/timestamp (for MVCC implementations).
     */
    long getCurrentVersion();

    /**
     * Optional: Support for transactions
     */
    default long beginTransaction() {
        throw new UnsupportedOperationException("Transactions not supported");
    }

    default Object readInTransaction(long txId, String key) {
        throw new UnsupportedOperationException("Transactions not supported");
    }

    default void commitTransaction(long txId) {
        throw new UnsupportedOperationException("Transactions not supported");
    }

    /**
     * Optional: Support for versioned reads (MVCC)
     */
    default Object readAtVersion(String key, long version) {
        throw new UnsupportedOperationException("Versioned reads not supported");
    }
}