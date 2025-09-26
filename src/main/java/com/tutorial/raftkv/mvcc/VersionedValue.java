package com.tutorial.raftkv.mvcc;

/**
 * Represents a versioned value in our MVCC system.
 * Each value has a version number (timestamp) and the actual data.
 */
public class VersionedValue {
    private final long version;
    private final String value;
    private final boolean deleted;

    public VersionedValue(long version, String value, boolean deleted) {
        this.version = version;
        this.value = value;
        this.deleted = deleted;
    }

    public VersionedValue(long version, String value) {
        this(version, value, false);
    }

    public long getVersion() {
        return version;
    }

    public String getValue() {
        return value;
    }

    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public String toString() {
        return String.format("VersionedValue{version=%d, value='%s', deleted=%s}",
            version, value, deleted);
    }
}