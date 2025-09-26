package com.tutorial.raftkv.core;

import com.tutorial.raftkv.mvcc.MVCCStore;
import com.tutorial.raftkv.raft.*;
import com.tutorial.raftkv.rpc.NettyRpcServer;
import com.tutorial.raftkv.rpc.NettyRpcClient;
import com.tutorial.raftkv.statemachine.StateMachine;
import com.tutorial.raftkv.statemachine.MVCCStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Raft node that combines:
 * - Raft consensus for coordinating multiple replicas
 * - MVCC store for handling concurrent operations
 * - RPC for inter-node communication
 *
 * This is where all three concepts (consensus, consistency, concurrency) come together.
 */
public class RaftNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final int port;
    private final Map<String, String> peerAddresses; // nodeId -> host:port

    // Core components
    private final StateMachine stateMachine;
    private final MVCCStateMachine mvccStateMachine; // Keep reference for MVCC-specific operations
    private final RaftCore raftCore;
    private final NettyRpcServer rpcServer;
    private final Map<String, NettyRpcClient> rpcClients;

    // Client request handling
    private final Map<String, CompletableFuture<String>> pendingRequests;

    public RaftNode(String nodeId, int port, Map<String, String> peerAddresses) {
        this.nodeId = nodeId;
        this.port = port;
        this.peerAddresses = new HashMap<>(peerAddresses);

        // Create the state machine (MVCC implementation)
        this.mvccStateMachine = new MVCCStateMachine();
        this.stateMachine = mvccStateMachine; // Use the generic interface

        this.pendingRequests = new ConcurrentHashMap<>();
        this.rpcClients = new ConcurrentHashMap<>();

        // Initialize Raft core with callback to apply commands to state machine
        this.raftCore = new RaftCore(nodeId, new ArrayList<>(peerAddresses.keySet()), this::applyCommand);

        // Initialize RPC server
        this.rpcServer = new NettyRpcServer(port, this);

        // Create RPC handler for Raft
        RaftCore.RpcHandler rpcHandler = new RaftCore.RpcHandler() {
            @Override
            public RaftCore.VoteResponse sendVoteRequest(String peer, RaftCore.VoteRequest request) {
                NettyRpcClient client = getOrCreateClient(peer);
                if (client != null) {
                    return client.sendVoteRequest(request);
                }
                return null;
            }

            @Override
            public RaftCore.AppendEntriesResponse sendAppendEntries(String peer, RaftCore.AppendEntriesRequest request) {
                NettyRpcClient client = getOrCreateClient(peer);
                if (client != null) {
                    return client.sendAppendEntries(request);
                }
                return null;
            }
        };

        raftCore.setRpcHandler(rpcHandler);
    }

    /**
     * Apply a committed command to the state machine.
     */
    private void applyCommand(LogEntry.Command command) {
        // Apply to the state machine (generic interface)
        stateMachine.apply(command);

        // Log the operation
        switch (command.getType()) {
            case PUT:
                logger.debug("{}: Applied PUT {} = {}", nodeId, command.getKey(), command.getValue());

                // Complete any pending request for this key
                String requestId = command.getKey() + "_put";
                CompletableFuture<String> future = pendingRequests.remove(requestId);
                if (future != null) {
                    future.complete("OK");
                }
                break;

            case DELETE:
                logger.debug("{}: Applied DELETE {}", nodeId, command.getKey());

                requestId = command.getKey() + "_delete";
                future = pendingRequests.remove(requestId);
                if (future != null) {
                    future.complete("OK");
                }
                break;

            case NO_OP:
                // No operation - used for Raft internals
                break;
        }
    }

    /**
     * Client API: Put a key-value pair.
     */
    public CompletableFuture<String> put(String key, String value) {
        if (raftCore.getState() != RaftState.LEADER) {
            String leader = raftCore.getCurrentLeader();
            return CompletableFuture.completedFuture("NOT_LEADER:" + (leader != null ? leader : "unknown"));
        }

        LogEntry.Command command = LogEntry.Command.put(key, value);
        CompletableFuture<Boolean> submitted = raftCore.submitCommand(command);

        if (submitted.join()) {
            CompletableFuture<String> result = new CompletableFuture<>();
            pendingRequests.put(key + "_put", result);
            return result;
        } else {
            return CompletableFuture.completedFuture("FAILED");
        }
    }

    /**
     * Client API: Get a value by key.
     * Uses state machine for consistent reads without blocking writes.
     */
    public String get(String key) {
        // Can read from any node (even followers) thanks to MVCC
        Object value = stateMachine.read(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Client API: Get a value at a specific version (snapshot read).
     */
    public String getAtVersion(String key, long version) {
        Object value = stateMachine.readAtVersion(key, version);
        return value != null ? value.toString() : null;
    }

    /**
     * Client API: Delete a key.
     */
    public CompletableFuture<String> delete(String key) {
        if (raftCore.getState() != RaftState.LEADER) {
            String leader = raftCore.getCurrentLeader();
            return CompletableFuture.completedFuture("NOT_LEADER:" + (leader != null ? leader : "unknown"));
        }

        LogEntry.Command command = LogEntry.Command.delete(key);
        CompletableFuture<Boolean> submitted = raftCore.submitCommand(command);

        if (submitted.join()) {
            CompletableFuture<String> result = new CompletableFuture<>();
            pendingRequests.put(key + "_delete", result);
            return result;
        } else {
            return CompletableFuture.completedFuture("FAILED");
        }
    }

    /**
     * Start a read transaction with snapshot isolation.
     */
    public long beginTransaction() {
        return stateMachine.beginTransaction();
    }

    /**
     * Read within a transaction (snapshot isolation).
     */
    public String getInTransaction(long txId, String key) {
        Object value = stateMachine.readInTransaction(txId, key);
        return value != null ? value.toString() : null;
    }

    /**
     * Commit a transaction.
     */
    public void commitTransaction(long txId) {
        stateMachine.commitTransaction(txId);
    }

    /**
     * Get all version history for a key (for educational purposes).
     * This requires the MVCC-specific implementation.
     */
    public List<com.tutorial.raftkv.mvcc.VersionedValue> getVersionHistory(String key) {
        return mvccStateMachine.getMvccStore().getVersionHistory(key);
    }

    /**
     * Get current MVCC version/timestamp.
     */
    public long getCurrentVersion() {
        return stateMachine.getCurrentVersion();
    }

    /**
     * Get the node ID.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Get node status information.
     */
    public NodeStatus getStatus() {
        return new NodeStatus(
            nodeId,
            raftCore.getState(),
            raftCore.getCurrentTerm(),
            raftCore.getCurrentLeader(),
            stateMachine.getCurrentVersion(),
            stateMachine.getSnapshot().size()
        );
    }

    /**
     * Start the node.
     */
    public void start() {
        logger.info("Starting node {} on port {}", nodeId, port);
        rpcServer.start();
        logger.info("Node {} started successfully", nodeId);
    }

    /**
     * Stop the node.
     */
    public void stop() {
        logger.info("Stopping node {}", nodeId);
        raftCore.shutdown();
        rpcServer.stop();
        for (NettyRpcClient client : rpcClients.values()) {
            client.close();
        }
        logger.info("Node {} stopped", nodeId);
    }

    /**
     * Get or create RPC client for a peer.
     */
    private NettyRpcClient getOrCreateClient(String peerId) {
        return rpcClients.computeIfAbsent(peerId, id -> {
            String address = peerAddresses.get(id);
            if (address != null) {
                String[] parts = address.split(":");
                return new NettyRpcClient(parts[0], Integer.parseInt(parts[1]));
            }
            return null;
        });
    }

    // Delegate methods for RPC server
    public RaftCore.VoteResponse handleVoteRequest(RaftCore.VoteRequest request) {
        return raftCore.requestVote(request);
    }

    public RaftCore.AppendEntriesResponse handleAppendEntries(RaftCore.AppendEntriesRequest request) {
        return raftCore.appendEntries(request);
    }

    /**
     * Node status information.
     */
    public static class NodeStatus {
        public final String nodeId;
        public final RaftState state;
        public final int currentTerm;
        public final String leader;
        public final long mvccVersion;
        public final int keyCount;

        public NodeStatus(String nodeId, RaftState state, int currentTerm, String leader,
                         long mvccVersion, int keyCount) {
            this.nodeId = nodeId;
            this.state = state;
            this.currentTerm = currentTerm;
            this.leader = leader;
            this.mvccVersion = mvccVersion;
            this.keyCount = keyCount;
        }

        @Override
        public String toString() {
            return String.format("Node %s: state=%s, term=%d, leader=%s, mvccVersion=%d, keys=%d",
                nodeId, state, currentTerm, leader, mvccVersion, keyCount);
        }
    }
}