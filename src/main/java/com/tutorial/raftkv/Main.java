package com.tutorial.raftkv;

import com.tutorial.raftkv.core.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Main entry point for running a single Raft node.
 *
 * Usage: java -jar mini-raft-kv.jar <nodeId> <port> <peer1:port1> <peer2:port2> ...
 *
 * Example for a 3-node cluster:
 *   java -jar mini-raft-kv.jar node1 5001 node2:5002 node3:5003
 *   java -jar mini-raft-kv.jar node2 5002 node1:5001 node3:5003
 *   java -jar mini-raft-kv.jar node3 5003 node1:5001 node2:5002
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java -jar mini-raft-kv.jar <nodeId> <port> [peer1:port1] [peer2:port2] ...");
            System.out.println("\nExample for 3-node cluster:");
            System.out.println("  Terminal 1: java -jar mini-raft-kv.jar node1 5001 node2:localhost:5002 node3:localhost:5003");
            System.out.println("  Terminal 2: java -jar mini-raft-kv.jar node2 5002 node1:localhost:5001 node3:localhost:5003");
            System.out.println("  Terminal 3: java -jar mini-raft-kv.jar node3 5003 node1:localhost:5001 node2:localhost:5002");
            System.out.println("\nOr run the demo with: gradle runCluster");
            System.exit(1);
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);

        // Parse peer addresses
        Map<String, String> peerAddresses = new HashMap<>();
        for (int i = 2; i < args.length; i++) {
            String[] parts = args[i].split(":");
            if (parts.length == 3) {
                // Format: peerId:host:port
                peerAddresses.put(parts[0], parts[1] + ":" + parts[2]);
            } else if (parts.length == 2) {
                // Format: peerId:port (assumes localhost)
                peerAddresses.put(parts[0], "localhost:" + parts[1]);
            }
        }

        logger.info("Starting node {} on port {} with peers: {}", nodeId, port, peerAddresses);

        // Create and start node
        RaftNode node = new RaftNode(nodeId, port, peerAddresses);
        node.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down node {}", nodeId);
            node.stop();
        }));

        // Keep running
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}