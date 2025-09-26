package com.tutorial.raftkv.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Core Raft consensus implementation.
 *
 * This implements the essential Raft algorithms:
 * - Leader election
 * - Log replication
 * - Safety guarantees
 */
public class RaftCore {
    private static final Logger logger = LoggerFactory.getLogger(RaftCore.class);

    // Node identity
    private final String nodeId;
    private final List<String> peers;

    // Persistent state (should be saved to disk in production)
    private int currentTerm = 0;
    private String votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();

    // Volatile state
    private final AtomicReference<RaftState> state = new AtomicReference<>(RaftState.FOLLOWER);
    private String currentLeader = null;

    // Leader state (reinitialized after election)
    private final Map<String, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();

    // Commit index and last applied
    private volatile int commitIndex = 0;
    private volatile int lastApplied = 0;

    // Election timeout management
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> electionTimeout;
    private final Random random = new Random();

    // Heartbeat interval (ms)
    private static final int HEARTBEAT_INTERVAL = 50;
    // Election timeout range (ms)
    private static final int MIN_ELECTION_TIMEOUT = 150;
    private static final int MAX_ELECTION_TIMEOUT = 300;

    // Callback for applying committed entries
    private final Consumer<LogEntry.Command> stateMachineCallback;

    // RPC handler (will be set when RPC is initialized)
    private RpcHandler rpcHandler;

    public RaftCore(String nodeId, List<String> peers, Consumer<LogEntry.Command> stateMachineCallback) {
        this.nodeId = nodeId;
        this.peers = new ArrayList<>(peers);
        this.stateMachineCallback = stateMachineCallback;

        // Add a no-op entry at index 0 (Raft log is 1-indexed)
        log.add(new LogEntry(0, LogEntry.Command.noOp(), 0));

        // Start as follower with election timeout
        resetElectionTimeout();
    }

    public void setRpcHandler(RpcHandler handler) {
        this.rpcHandler = handler;
    }

    /**
     * Request Vote RPC - called by candidates during elections.
     */
    public synchronized VoteResponse requestVote(VoteRequest request) {
        logger.debug("{}: Received vote request from {} for term {}", nodeId, request.candidateId, request.term);

        // If request term is greater, become follower
        if (request.term > currentTerm) {
            currentTerm = request.term;
            votedFor = null;
            state.set(RaftState.FOLLOWER);
        }

        // Reject if we've already voted for someone else in this term
        if (votedFor != null && !votedFor.equals(request.candidateId)) {
            return new VoteResponse(currentTerm, false);
        }

        // Check if candidate's log is at least as up-to-date as ours
        int lastLogIndex = log.size() - 1;
        int lastLogTerm = log.get(lastLogIndex).getTerm();

        boolean logIsUpToDate = request.lastLogTerm > lastLogTerm ||
            (request.lastLogTerm == lastLogTerm && request.lastLogIndex >= lastLogIndex);

        if (request.term >= currentTerm && logIsUpToDate) {
            votedFor = request.candidateId;
            resetElectionTimeout();
            logger.info("{}: Voted for {} in term {}", nodeId, request.candidateId, currentTerm);
            return new VoteResponse(currentTerm, true);
        }

        return new VoteResponse(currentTerm, false);
    }

    /**
     * Append Entries RPC - used for both heartbeats and log replication.
     */
    public synchronized AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        // If request term is greater, become follower
        if (request.term > currentTerm) {
            currentTerm = request.term;
            votedFor = null;
            state.set(RaftState.FOLLOWER);
        }

        // Reject if term is old
        if (request.term < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false);
        }

        // Reset election timeout (we heard from a valid leader)
        resetElectionTimeout();
        currentLeader = request.leaderId;
        state.set(RaftState.FOLLOWER);

        // Check if log matches at prevLogIndex
        if (request.prevLogIndex > 0) {
            if (log.size() <= request.prevLogIndex ||
                log.get(request.prevLogIndex).getTerm() != request.prevLogTerm) {
                return new AppendEntriesResponse(currentTerm, false);
            }
        }

        // Append new entries
        if (!request.entries.isEmpty()) {
            // Remove conflicting entries
            int i = request.prevLogIndex + 1;
            for (LogEntry entry : request.entries) {
                if (i < log.size() && log.get(i).getTerm() != entry.getTerm()) {
                    // Remove this and all following entries
                    while (log.size() > i) {
                        log.remove(log.size() - 1);
                    }
                }
                if (i >= log.size()) {
                    log.add(entry);
                }
                i++;
            }
            logger.debug("{}: Appended {} entries", nodeId, request.entries.size());
        }

        // Update commit index
        if (request.leaderCommit > commitIndex) {
            commitIndex = Math.min(request.leaderCommit, log.size() - 1);
            applyCommittedEntries();
        }

        return new AppendEntriesResponse(currentTerm, true);
    }

    /**
     * Start election process.
     */
    private synchronized void startElection() {
        currentTerm++;
        state.set(RaftState.CANDIDATE);
        votedFor = nodeId;
        resetElectionTimeout();

        logger.info("{}: Starting election for term {}", nodeId, currentTerm);

        final int term = currentTerm;
        final int lastLogIndex = log.size() - 1;
        final int lastLogTerm = log.get(lastLogIndex).getTerm();

        AtomicInteger votes = new AtomicInteger(1); // Vote for self
        int majority = (peers.size() + 1) / 2 + 1;

        // Request votes from all peers in parallel
        CountDownLatch voteLatch = new CountDownLatch(peers.size());

        for (String peer : peers) {
            CompletableFuture.runAsync(() -> {
                try {
                    if (rpcHandler != null) {
                        VoteRequest request = new VoteRequest(term, nodeId, lastLogIndex, lastLogTerm);
                        VoteResponse response = rpcHandler.sendVoteRequest(peer, request);

                        if (response != null && response.voteGranted) {
                            int voteCount = votes.incrementAndGet();
                            logger.debug("{}: Received vote from {}, total votes: {}", nodeId, peer, voteCount);

                            // Become leader if we have majority
                            if (voteCount >= majority && state.get() == RaftState.CANDIDATE && currentTerm == term) {
                                becomeLeader();
                            }
                        } else if (response != null && response.term > currentTerm) {
                            synchronized (RaftCore.this) {
                                currentTerm = response.term;
                                state.set(RaftState.FOLLOWER);
                                votedFor = null;
                            }
                        }
                    }
                } finally {
                    voteLatch.countDown();
                }
            });
        }

        // Wait for votes (with timeout)
        try {
            voteLatch.await(MIN_ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Transition to leader state.
     */
    private synchronized void becomeLeader() {
        if (state.get() != RaftState.CANDIDATE) {
            return;
        }

        logger.info("{}: Became leader for term {}", nodeId, currentTerm);
        state.set(RaftState.LEADER);
        currentLeader = nodeId;

        // Initialize leader state
        for (String peer : peers) {
            nextIndex.put(peer, log.size());
            matchIndex.put(peer, 0);
        }

        // Send initial heartbeats
        sendHeartbeats();

        // Schedule periodic heartbeats
        scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    /**
     * Send heartbeats/append entries to all followers.
     */
    private void sendHeartbeats() {
        if (state.get() != RaftState.LEADER) {
            return;
        }

        for (String peer : peers) {
            sendAppendEntries(peer);
        }
    }

    /**
     * Send append entries to a specific peer.
     */
    private void sendAppendEntries(String peer) {
        if (state.get() != RaftState.LEADER || rpcHandler == null) {
            return;
        }

        int nextIdx = nextIndex.getOrDefault(peer, 1);
        int prevLogIndex = nextIdx - 1;
        int prevLogTerm = prevLogIndex > 0 ? log.get(prevLogIndex).getTerm() : 0;

        List<LogEntry> entries = new ArrayList<>();
        if (nextIdx < log.size()) {
            entries = new ArrayList<>(log.subList(nextIdx, log.size()));
        }

        AppendEntriesRequest request = new AppendEntriesRequest(
            currentTerm, nodeId, prevLogIndex, prevLogTerm, entries, commitIndex
        );

        CompletableFuture.runAsync(() -> {
            AppendEntriesResponse response = rpcHandler.sendAppendEntries(peer, request);
            if (response != null) {
                handleAppendEntriesResponse(peer, nextIdx, response);
            }
        });
    }

    /**
     * Handle response to append entries RPC.
     */
    private synchronized void handleAppendEntriesResponse(String peer, int nextIdx, AppendEntriesResponse response) {
        if (response.term > currentTerm) {
            currentTerm = response.term;
            state.set(RaftState.FOLLOWER);
            votedFor = null;
            return;
        }

        if (state.get() != RaftState.LEADER) {
            return;
        }

        if (response.success) {
            // Update nextIndex and matchIndex
            nextIndex.put(peer, log.size());
            matchIndex.put(peer, log.size() - 1);

            // Update commit index
            updateCommitIndex();
        } else {
            // Decrement nextIndex and retry
            int newNextIndex = Math.max(1, nextIndex.get(peer) - 1);
            nextIndex.put(peer, newNextIndex);
        }
    }

    /**
     * Update commit index based on majority replication.
     */
    private void updateCommitIndex() {
        for (int n = log.size() - 1; n > commitIndex; n--) {
            if (log.get(n).getTerm() == currentTerm) {
                int replicas = 1; // Count self
                for (int matchIdx : matchIndex.values()) {
                    if (matchIdx >= n) {
                        replicas++;
                    }
                }
                if (replicas > (peers.size() + 1) / 2) {
                    commitIndex = n;
                    applyCommittedEntries();
                    break;
                }
            }
        }
    }

    /**
     * Apply committed entries to the state machine.
     */
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            if (stateMachineCallback != null) {
                stateMachineCallback.accept(entry.getCommand());
            }
            logger.debug("{}: Applied entry at index {}", nodeId, lastApplied);
        }
    }

    /**
     * Reset the election timeout.
     */
    private void resetElectionTimeout() {
        if (electionTimeout != null) {
            electionTimeout.cancel(false);
        }

        if (state.get() != RaftState.LEADER) {
            int timeout = MIN_ELECTION_TIMEOUT + random.nextInt(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT);
            electionTimeout = scheduler.schedule(this::handleElectionTimeout, timeout, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Handle election timeout - start new election.
     */
    private void handleElectionTimeout() {
        if (state.get() != RaftState.LEADER) {
            logger.debug("{}: Election timeout - starting election", nodeId);
            startElection();
        }
    }

    /**
     * Submit a new command to be replicated (leader only).
     */
    public synchronized CompletableFuture<Boolean> submitCommand(LogEntry.Command command) {
        if (state.get() != RaftState.LEADER) {
            return CompletableFuture.completedFuture(false);
        }

        // Append to local log
        LogEntry entry = new LogEntry(currentTerm, command, log.size());
        log.add(entry);
        logger.info("{}: Added command to log at index {}", nodeId, log.size() - 1);

        // Replicate to followers
        sendHeartbeats();

        return CompletableFuture.completedFuture(true);
    }

    // Getters
    public RaftState getState() {
        return state.get();
    }

    public String getCurrentLeader() {
        return currentLeader;
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }

    // RPC message classes
    public static class VoteRequest {
        public final int term;
        public final String candidateId;
        public final int lastLogIndex;
        public final int lastLogTerm;

        public VoteRequest(int term, String candidateId, int lastLogIndex, int lastLogTerm) {
            this.term = term;
            this.candidateId = candidateId;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }
    }

    public static class VoteResponse {
        public final int term;
        public final boolean voteGranted;

        public VoteResponse(int term, boolean voteGranted) {
            this.term = term;
            this.voteGranted = voteGranted;
        }
    }

    public static class AppendEntriesRequest {
        public final int term;
        public final String leaderId;
        public final int prevLogIndex;
        public final int prevLogTerm;
        public final List<LogEntry> entries;
        public final int leaderCommit;

        public AppendEntriesRequest(int term, String leaderId, int prevLogIndex, int prevLogTerm,
                                    List<LogEntry> entries, int leaderCommit) {
            this.term = term;
            this.leaderId = leaderId;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entries = entries;
            this.leaderCommit = leaderCommit;
        }
    }

    public static class AppendEntriesResponse {
        public final int term;
        public final boolean success;

        public AppendEntriesResponse(int term, boolean success) {
            this.term = term;
            this.success = success;
        }
    }

    // RPC handler interface
    public interface RpcHandler {
        VoteResponse sendVoteRequest(String peer, VoteRequest request);
        AppendEntriesResponse sendAppendEntries(String peer, AppendEntriesRequest request);
    }

}