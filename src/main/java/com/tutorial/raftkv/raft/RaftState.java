package com.tutorial.raftkv.raft;

/**
 * The three states a Raft node can be in.
 */
public enum RaftState {
    FOLLOWER,   // Most nodes most of the time
    CANDIDATE,  // During elections
    LEADER      // Exactly one per term
}