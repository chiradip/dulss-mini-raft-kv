package com.tutorial.raftkv.raft;

import java.io.Serializable;

/**
 * A single entry in the Raft log.
 * Contains the command to be applied to the state machine and the term when it was received.
 */
public class LogEntry implements Serializable {
    private final int term;
    private final Command command;
    private final long index;

    public LogEntry(int term, Command command, long index) {
        this.term = term;
        this.command = command;
        this.index = index;
    }

    public int getTerm() {
        return term;
    }

    public Command getCommand() {
        return command;
    }

    public long getIndex() {
        return index;
    }

    /**
     * Command types that can be replicated through Raft.
     */
    public static class Command implements Serializable {
        public enum Type {
            PUT, DELETE, NO_OP
        }

        private final Type type;
        private final String key;
        private final String value;

        public Command(Type type, String key, String value) {
            this.type = type;
            this.key = key;
            this.value = value;
        }

        public static Command put(String key, String value) {
            return new Command(Type.PUT, key, value);
        }

        public static Command delete(String key) {
            return new Command(Type.DELETE, key, null);
        }

        public static Command noOp() {
            return new Command(Type.NO_OP, null, null);
        }

        public Type getType() {
            return type;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.format("Command{type=%s, key='%s', value='%s'}", type, key, value);
        }
    }

    @Override
    public String toString() {
        return String.format("LogEntry{term=%d, index=%d, command=%s}", term, index, command);
    }
}