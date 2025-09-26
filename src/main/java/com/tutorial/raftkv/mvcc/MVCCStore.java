package com.tutorial.raftkv.mvcc;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-Version Concurrency Control (MVCC) Key-Value Store.
 *
 * Key concepts:
 * - Each write creates a new version with a timestamp
 * - Reads can access any historical version
 * - No locking needed - readers don't block writers
 */
public class MVCCStore {
    // Key -> List of all versions (sorted by version number)
    private final Map<String, List<VersionedValue>> store;

    // Global version counter (simulating a timestamp)
    private final AtomicLong versionCounter;

    // Transaction ID counter
    private final AtomicLong transactionCounter;

    // Active transactions and their read timestamps
    private final Map<Long, Long> activeTransactions;

    public MVCCStore() {
        this.store = new ConcurrentHashMap<>();
        this.versionCounter = new AtomicLong(0);
        this.transactionCounter = new AtomicLong(0);
        this.activeTransactions = new ConcurrentHashMap<>();
    }

    /**
     * Start a new transaction for reading at current version.
     */
    public long beginTransaction() {
        long txId = transactionCounter.incrementAndGet();
        long readVersion = versionCounter.get();
        activeTransactions.put(txId, readVersion);
        return txId;
    }

    /**
     * Commit a transaction (cleanup).
     */
    public void commitTransaction(long txId) {
        activeTransactions.remove(txId);
    }

    /**
     * Write a new value (creates a new version).
     */
    public long put(String key, String value) {
        long version = versionCounter.incrementAndGet();
        VersionedValue versionedValue = new VersionedValue(version, value);

        store.compute(key, (k, versions) -> {
            if (versions == null) {
                versions = new ArrayList<>();
            }
            versions.add(versionedValue);
            return versions;
        });

        return version;
    }

    /**
     * Delete a key (creates a tombstone version).
     */
    public long delete(String key) {
        long version = versionCounter.incrementAndGet();
        VersionedValue tombstone = new VersionedValue(version, null, true);

        store.compute(key, (k, versions) -> {
            if (versions == null) {
                versions = new ArrayList<>();
            }
            versions.add(tombstone);
            return versions;
        });

        return version;
    }

    /**
     * Read the latest committed value.
     */
    public String get(String key) {
        return getAtVersion(key, versionCounter.get());
    }

    /**
     * Read value at a specific version (snapshot read).
     */
    public String getAtVersion(String key, long version) {
        List<VersionedValue> versions = store.get(key);
        if (versions == null || versions.isEmpty()) {
            return null;
        }

        // Find the latest version <= requested version
        VersionedValue result = null;
        for (VersionedValue v : versions) {
            if (v.getVersion() <= version) {
                result = v;
            } else {
                break;
            }
        }

        if (result == null || result.isDeleted()) {
            return null;
        }
        return result.getValue();
    }

    /**
     * Read value within a transaction (uses transaction's read version).
     */
    public String getInTransaction(long txId, String key) {
        Long readVersion = activeTransactions.get(txId);
        if (readVersion == null) {
            throw new IllegalStateException("Transaction " + txId + " not found");
        }
        return getAtVersion(key, readVersion);
    }

    /**
     * Get all versions of a key (for debugging/education).
     */
    public List<VersionedValue> getVersionHistory(String key) {
        List<VersionedValue> versions = store.get(key);
        return versions != null ? new ArrayList<>(versions) : Collections.emptyList();
    }

    /**
     * Garbage collection: Remove old versions that no active transaction can see.
     */
    public void garbageCollect() {
        long minActiveVersion = activeTransactions.values().stream()
            .min(Long::compare)
            .orElse(Long.MAX_VALUE);

        store.forEach((key, versions) -> {
            if (versions.size() <= 1) return;

            // Keep only the latest version before minActiveVersion and all after
            List<VersionedValue> toKeep = new ArrayList<>();
            VersionedValue latestBefore = null;

            for (VersionedValue v : versions) {
                if (v.getVersion() < minActiveVersion) {
                    latestBefore = v;
                } else {
                    if (latestBefore != null) {
                        toKeep.add(latestBefore);
                        latestBefore = null;
                    }
                    toKeep.add(v);
                }
            }

            if (latestBefore != null) {
                toKeep.add(0, latestBefore);
            }

            versions.clear();
            versions.addAll(toKeep);
        });
    }

    public long getCurrentVersion() {
        return versionCounter.get();
    }

    /**
     * Get a snapshot of all current key-value pairs.
     */
    public Map<String, String> snapshot() {
        Map<String, String> result = new HashMap<>();
        long currentVersion = versionCounter.get();

        store.forEach((key, versions) -> {
            String value = getAtVersion(key, currentVersion);
            if (value != null) {
                result.put(key, value);
            }
        });

        return result;
    }
}