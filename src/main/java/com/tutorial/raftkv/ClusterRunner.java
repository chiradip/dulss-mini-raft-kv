package com.tutorial.raftkv;

import com.tutorial.raftkv.core.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Flexible cluster runner that starts a configurable number of Raft nodes.
 *
 * Usage:
 *   gradle runCluster                    # Runs with default 3 nodes
 *   gradle runCluster --args="5"         # Runs with 5 nodes
 *   gradle runCluster --args="7 6000"    # Runs 7 nodes starting at port 6000
 */
public class ClusterRunner {
    private static final Logger logger = LoggerFactory.getLogger(ClusterRunner.class);
    private static final int DEFAULT_NODE_COUNT = 3;
    private static final int DEFAULT_BASE_PORT = 5000;

    public static void main(String[] args) {
        // Parse arguments
        int nodeCount = DEFAULT_NODE_COUNT;
        int basePort = DEFAULT_BASE_PORT;

        if (args.length > 0) {
            try {
                nodeCount = Integer.parseInt(args[0]);
                if (nodeCount < 1 || nodeCount > 20) {
                    System.err.println("Node count must be between 1 and 20");
                    System.exit(1);
                }
            } catch (NumberFormatException e) {
                System.err.println("Invalid node count: " + args[0]);
                System.exit(1);
            }
        }

        if (args.length > 1) {
            try {
                basePort = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid base port: " + args[1]);
                System.exit(1);
            }
        }

        System.out.println("========================================");
        System.out.println("Starting Raft-KV Cluster");
        System.out.println("========================================");
        System.out.println("Nodes: " + nodeCount);
        System.out.println("Base Port: " + basePort);
        System.out.println("Port Range: " + (basePort + 1) + " - " + (basePort + nodeCount));
        System.out.println("========================================\n");

        // Create node configurations
        Map<String, Integer> nodeConfigs = new LinkedHashMap<>();
        for (int i = 1; i <= nodeCount; i++) {
            String nodeId = "node" + i;
            int port = basePort + i;
            nodeConfigs.put(nodeId, port);
        }

        // Start all nodes
        Map<String, RaftNode> nodes = new HashMap<>();

        for (Map.Entry<String, Integer> entry : nodeConfigs.entrySet()) {
            String nodeId = entry.getKey();
            int port = entry.getValue();

            // Create peer addresses (all nodes except self)
            Map<String, String> peerAddresses = new HashMap<>();
            for (Map.Entry<String, Integer> peer : nodeConfigs.entrySet()) {
                if (!peer.getKey().equals(nodeId)) {
                    peerAddresses.put(peer.getKey(), "localhost:" + peer.getValue());
                }
            }

            // Create and start node
            RaftNode node = new RaftNode(nodeId, port, peerAddresses);
            nodes.put(nodeId, node);
            node.start();
            logger.info("Started {} on port {}", nodeId, port);
        }

        System.out.println("\n========================================");
        System.out.println("Cluster is RUNNING");
        System.out.println("========================================");
        System.out.println("\nConnect clients to any of these addresses:");
        for (Map.Entry<String, Integer> entry : nodeConfigs.entrySet()) {
            System.out.println("  - localhost:" + entry.getValue() + " (" + entry.getKey() + ")");
        }

        System.out.println("\nTo connect a client:");
        System.out.println("  gradle runClient");
        System.out.println("\nPress Ctrl+C to shutdown the cluster");
        System.out.println("========================================\n");

        // Add shutdown hook for clean shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n\nShutting down cluster...");
            for (RaftNode node : nodes.values()) {
                node.stop();
            }
            System.out.println("Cluster shutdown complete.");
        }));

        // Keep the cluster running
        try {
            new CountDownLatch(1).await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}