package com.tutorial.raftkv;

import com.tutorial.raftkv.client.NetworkKVClient;
import java.util.*;

/**
 * Standalone client that connects to a running Raft cluster.
 *
 * Usage:
 * 1. First start the cluster nodes (in separate terminals or using Main.java)
 * 2. Then run this client to interact with the cluster
 */
public class ClientRunner {
    public static void main(String[] args) {
        System.out.println("=================================");
        System.out.println("   Raft KV Store Client");
        System.out.println("=================================\n");

        // Parse command line arguments for cluster addresses
        List<String> clusterAddresses = new ArrayList<>();

        if (args.length > 0) {
            // Check if first argument is a number (node count)
            try {
                int nodeCount = Integer.parseInt(args[0]);
                int basePort = args.length > 1 ? Integer.parseInt(args[1]) : 5000;

                // Generate addresses for the specified number of nodes
                System.out.println("Connecting to " + nodeCount + "-node cluster at base port " + basePort);
                for (int i = 1; i <= nodeCount; i++) {
                    clusterAddresses.add("localhost:" + (basePort + i));
                }
            } catch (NumberFormatException e) {
                // Not a number, treat as explicit addresses
                clusterAddresses.addAll(Arrays.asList(args));
            }
        } else {
            // Try to detect running cluster by checking common ports
            System.out.println("Auto-detecting cluster configuration...");

            // Try common configurations
            int[] commonPorts = {5001, 5002, 5003, 5004, 5005};
            for (int port : commonPorts) {
                clusterAddresses.add("localhost:" + port);
            }

            System.out.println("Trying to connect to nodes at ports: 5001-5005");
            System.out.println("Use 'gradle runClient --args=\"<nodeCount> <basePort>\"' to specify different configuration\n");
        }

        // Create network client
        NetworkKVClient client = new NetworkKVClient(clusterAddresses);

        // Try to connect
        System.out.println("Connecting to cluster...");
        if (!client.connect()) {
            System.err.println("Failed to connect to any node in the cluster!");
            System.err.println("Make sure the cluster is running:");
            System.err.println("  Terminal 1: ./gradlew run --args=\"node1 5001 node2:5002 node3:5003\"");
            System.err.println("  Terminal 2: ./gradlew run --args=\"node2 5002 node1:5001 node3:5003\"");
            System.err.println("  Terminal 3: ./gradlew run --args=\"node3 5003 node1:5001 node2:5002\"");
            System.exit(1);
        }

        System.out.println("Connected to cluster!\n");

        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        Long currentTxId = null;  // Track active transaction

        System.out.println("Available commands:");
        System.out.println("  put <key> <value>  - Store a key-value pair");
        System.out.println("  get <key>          - Retrieve a value");
        System.out.println("  delete <key>       - Delete a key");
        System.out.println("  begin              - Start a transaction (snapshot isolation)");
        System.out.println("  tget <key>         - Read within transaction");
        System.out.println("  commit             - Commit transaction");
        System.out.println("  txdemo             - Run transaction isolation demo");
        System.out.println("  status             - Show cluster status");
        System.out.println("  leader             - Show current leader");
        System.out.println("  help               - Show this help");
        System.out.println("  quit               - Exit\n");

        while (running) {
            System.out.print("> ");
            String line = scanner.nextLine().trim();

            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+", 3);
            String command = parts[0].toLowerCase();

            try {
                switch (command) {
                    case "put":
                        if (parts.length < 3) {
                            System.out.println("Usage: put <key> <value>");
                        } else {
                            String result = client.put(parts[1], parts[2]);
                            System.out.println("Result: " + result);
                        }
                        break;

                    case "get":
                        if (parts.length < 2) {
                            System.out.println("Usage: get <key>");
                        } else {
                            String value = client.get(parts[1]);
                            if (value != null && !value.equals("NOT_FOUND") && !value.startsWith("ERROR:")) {
                                System.out.println(parts[1] + " = " + value);
                            } else if (value != null && value.equals("NOT_FOUND")) {
                                System.out.println(parts[1] + " = (not found)");
                            } else {
                                System.out.println("Error: " + value);
                            }
                        }
                        break;

                    case "delete":
                        if (parts.length < 2) {
                            System.out.println("Usage: delete <key>");
                        } else {
                            String result = client.delete(parts[1]);
                            System.out.println("Result: " + result);
                        }
                        break;

                    case "status":
                        String status = client.getStatus();
                        System.out.println(status);
                        break;

                    case "begin":
                        if (currentTxId != null) {
                            System.out.println("Transaction already in progress (ID: " + currentTxId + ")");
                        } else {
                            String txResult = client.beginTransaction();
                            if (txResult != null && !txResult.startsWith("ERROR")) {
                                currentTxId = Long.parseLong(txResult);
                                System.out.println("Transaction " + currentTxId + " started");
                            } else {
                                System.out.println("Failed to start transaction: " + txResult);
                            }
                        }
                        break;

                    case "tget":
                        if (currentTxId == null) {
                            System.out.println("No active transaction. Use 'begin' to start one.");
                        } else if (parts.length < 2) {
                            System.out.println("Usage: tget <key>");
                        } else {
                            String value = client.getInTransaction(currentTxId, parts[1]);
                            if (value != null && !value.equals("NOT_FOUND") && !value.startsWith("ERROR:")) {
                                System.out.println(parts[1] + " = " + value + " (tx: " + currentTxId + ")");
                            } else if (value != null && value.equals("NOT_FOUND")) {
                                System.out.println(parts[1] + " = (not found in transaction)");
                            } else {
                                System.out.println("Error: " + value);
                            }
                        }
                        break;

                    case "commit":
                        if (currentTxId == null) {
                            System.out.println("No active transaction to commit");
                        } else {
                            String commitResult = client.commitTransaction(currentTxId);
                            System.out.println("Transaction " + currentTxId + " committed");
                            currentTxId = null;
                        }
                        break;

                    case "txdemo":
                        runTransactionDemo(client);
                        break;

                    case "leader":
                        String leader = client.getLeader();
                        System.out.println("Current leader: " + (leader != null ? leader : "unknown"));
                        break;

                    case "help":
                        System.out.println("\nAvailable commands:");
                        System.out.println("  put <key> <value>  - Store a key-value pair");
                        System.out.println("  get <key>          - Retrieve a value");
                        System.out.println("  delete <key>       - Delete a key");
                        System.out.println("  begin              - Start a transaction (snapshot isolation)");
                        System.out.println("  tget <key>         - Read within transaction");
                        System.out.println("  commit             - Commit transaction");
                        System.out.println("  txdemo             - Run transaction isolation demo");
                        System.out.println("  status             - Show cluster status");
                        System.out.println("  leader             - Show current leader");
                        System.out.println("  help               - Show this help");
                        System.out.println("  quit               - Exit");
                        break;

                    case "quit":
                    case "exit":
                        running = false;
                        break;

                    default:
                        System.out.println("Unknown command: " + command);
                        System.out.println("Type 'help' for available commands");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }

        System.out.println("\nDisconnecting from cluster...");
        client.close();
        System.out.println("Goodbye!");
    }

    private static void runTransactionDemo(NetworkKVClient client) {
        System.out.println("\n=== MVCC Transaction Isolation Demo ===\n");

        try {
            // Setup initial value
            System.out.println("1. Setting up initial value:");
            System.out.println("   > put balance 100");
            client.put("balance", "100");
            System.out.println("   Result: OK\n");

            // Start transaction
            System.out.println("2. Starting a transaction (captures snapshot):");
            String txResult = client.beginTransaction();
            Long txId = Long.parseLong(txResult);
            System.out.println("   Transaction " + txId + " started\n");

            // Read in transaction
            System.out.println("3. Reading balance in transaction:");
            String txValue = client.getInTransaction(txId, "balance");
            System.out.println("   Transaction sees: balance = " + txValue + "\n");

            // Modify value outside transaction
            System.out.println("4. Another client updates balance (outside transaction):");
            System.out.println("   > put balance 200");
            client.put("balance", "200");
            System.out.println("   Result: OK\n");

            // Read again in transaction - should still see old value
            System.out.println("5. Transaction reads balance again:");
            txValue = client.getInTransaction(txId, "balance");
            System.out.println("   Transaction STILL sees: balance = " + txValue);
            System.out.println("   (Snapshot isolation - transaction sees consistent view!)\n");

            // Read outside transaction - sees new value
            System.out.println("6. Read outside transaction:");
            String currentValue = client.get("balance");
            System.out.println("   Current value: balance = " + currentValue + "\n");

            // Commit transaction
            System.out.println("7. Committing transaction:");
            client.commitTransaction(txId);
            System.out.println("   Transaction " + txId + " committed\n");

            System.out.println("=== Demo Complete ===");
            System.out.println("Key Insight: Transactions see a consistent snapshot from when they started,");
            System.out.println("even if other clients modify data. This is MVCC in action!\n");

        } catch (Exception e) {
            System.out.println("Demo failed: " + e.getMessage());
        }
    }
}