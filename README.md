# Mini Raft-KV Store: Understanding Consensus, Consistency, and Concurrency

A complete, educational implementation of a distributed key-value store using Raft consensus and MVCC (Multi-Version Concurrency Control) in Java.

## 🎯 What This Project Teaches

This project demonstrates how three fundamental distributed systems concepts work together:

1. **Consensus (Raft)** - How multiple nodes agree on a single value
2. **Consistency** - How to maintain data integrity across replicas
3. **Concurrency (MVCC)** - How to handle simultaneous operations without locking

## 📚 Core Concepts

### Consensus with Raft
Raft ensures all nodes in a cluster agree on the same sequence of operations:
- **Leader Election**: Nodes elect a leader to coordinate all writes
- **Log Replication**: Leader replicates commands to followers
- **Safety**: Guarantees that committed entries are never lost

### MVCC for Concurrency
MVCC allows multiple transactions to read and write simultaneously without locking:
- **Versioning**: Each write creates a new version with a timestamp
- **Snapshot Isolation**: Readers see a consistent snapshot of data
- **No Locking**: Readers never block writers, writers never block readers

### How They Work Together
1. **Writes go through Raft** for consensus across nodes
2. **Once committed**, writes are applied to the MVCC store
3. **Reads use MVCC** for consistent, lock-free access

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Node 1    │     │   Node 2    │     │   Node 3    │
│  (Leader)   │────▶│ (Follower)  │────▶│ (Follower)  │
└─────┬───────┘     └─────┬───────┘     └─────┬───────┘
      │                   │                   │
      ▼                   ▼                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ MVCC Store  │     │ MVCC Store  │     │ MVCC Store  │
│ (Versions)  │     │ (Versions)  │     │ (Versions)  │
└─────────────┘     └─────────────┘     └─────────────┘
```

## 🚀 Quick Start

### Build the Project
```bash
cd mini-raft-kv
./gradlew build
```

### Option 1: Run a Cluster (Easiest)
```bash
# Start a 3-node cluster (default)
./gradlew runCluster

# Start a 5-node cluster
./gradlew runCluster --args="5"

# Start a 7-node cluster on custom ports
./gradlew runCluster --args="7 6000"
```

The cluster will display:
- All node addresses for client connections
- Leader election progress
- Instructions for connecting clients

### Option 2: Connect a Client

In another terminal, connect to the running cluster:
```bash
# Auto-detect cluster (tries common ports)
./gradlew runClient

# Connect to specific cluster size
./gradlew runClient --args="5"        # 5-node cluster
./gradlew runClient --args="7 6000"   # 7-node cluster on port 6000+
```

### Client Commands
Once connected, you can use these commands:
```
> put name Alice           # Store key-value pair
> get name                 # Retrieve value
> delete name              # Delete key
> begin                    # Start transaction
> tget name               # Read in transaction
> commit                   # Commit transaction
> txdemo                   # Run MVCC demo
> status                   # Show cluster status
> leader                   # Show current leader
> help                     # Show all commands
> quit                     # Exit
```

## 📖 Code Walkthrough

### Key Components

**Consensus Layer** (`raft/` package):
- `RaftCore.java` - Complete Raft consensus algorithm
- `LogEntry.java` - Commands replicated across nodes
- `RaftState.java` - Node states (Leader, Follower, Candidate)

**Storage Layer** (`mvcc/` package):
- `MVCCStore.java` - Multi-version storage implementation
- `VersionedValue.java` - Values with version numbers

**Integration** (`core/` package):
- `RaftNode.java` - Combines Raft + MVCC + RPC
- `ClusterRunner.java` - Configurable cluster launcher

**State Machine** (`statemachine/` package):
- `StateMachine.java` - Generic interface for replicated state
- `MVCCStateMachine.java` - MVCC implementation of state machine

**Networking** (`rpc/` package):
- `NettyRpcServer.java` - Handles RPC requests
- `NettyRpcClient.java` - Sends RPC requests

**Client** (`client/` package):
- `NetworkKVClient.java` - Network client library
- `ClientRunner.java` - Interactive CLI client

### Understanding the Flow

1. **Client Write Request**
   ```
   Client.put("key", "value")
     → Goes to Leader (redirected if needed)
     → Leader adds to log
     → Replicates to followers via AppendEntries RPC
     → Waits for majority acknowledgment
     → Commits entry
     → Applies to MVCC store
     → Returns success to client
   ```

2. **Client Read Request**
   ```
   Client.get("key")
     → Can go to any node
     → Reads from local MVCC store
     → Returns value at latest version
   ```

3. **Transaction Flow**
   ```
   Client.beginTransaction()
     → Creates snapshot at current version
   Client.getInTransaction(txId, "key")
     → Reads from snapshot version
   Client.commitTransaction(txId)
     → Transaction completes
   ```

## 🔬 Key Learning Points

### Watch Leader Election
During cluster startup, you'll see:
```
node1: Starting election for term 1
node2: Voted for node1 in term 1
node3: Voted for node1 in term 1
node1: Became leader for term 1
```

### Observe MVCC Versions
The transaction demo shows version isolation:
```
1. Setting up initial value: balance = 100
2. Transaction starts (sees version 1)
3. Outside update: balance = 200 (creates version 2)
4. Transaction still sees: balance = 100 (snapshot isolation!)
```

### Test Fault Tolerance
1. Start a cluster with `./gradlew runCluster`
2. Connect client with `./gradlew runClient`
3. Check leader with `> leader` command
4. Kill the leader node (Ctrl+C in cluster terminal)
5. Watch new leader election happen
6. Client continues working with new leader

## 🏛️ Project Structure

```
mini-raft-kv/
├── src/main/java/com/tutorial/raftkv/
│   ├── mvcc/                  # MVCC implementation
│   │   ├── MVCCStore.java
│   │   └── VersionedValue.java
│   ├── raft/                  # Raft consensus
│   │   ├── RaftCore.java
│   │   ├── LogEntry.java
│   │   └── RaftState.java
│   ├── core/                  # Integration
│   │   ├── RaftNode.java
│   │   └── ClusterRunner.java
│   ├── statemachine/          # Generic state machine
│   │   ├── StateMachine.java
│   │   └── MVCCStateMachine.java
│   ├── rpc/                   # Networking
│   │   ├── NettyRpcServer.java
│   │   └── NettyRpcClient.java
│   ├── client/                # Client implementation
│   │   └── NetworkKVClient.java
│   ├── ClientRunner.java      # Interactive CLI
│   └── Main.java              # Single node runner
└── build.gradle               # Build configuration
```

## 🎓 Educational Exercises

### Exercise 1: Scale the Cluster
```bash
# Try different cluster sizes
./gradlew runCluster --args="5"   # 5 nodes
./gradlew runCluster --args="7"   # 7 nodes
./gradlew runCluster --args="9"   # 9 nodes

# Observe:
# - How does election time change?
# - What's the quorum size for each?
# - How does write latency vary?
```

### Exercise 2: Test Split Brain
1. Start a 5-node cluster
2. Identify the leader
3. "Partition" the network by killing 3 nodes
4. Observe that remaining 2 nodes cannot elect leader (no majority)
5. This prevents split-brain scenarios

### Exercise 3: Benchmark Performance
```bash
# In the client, time operations:
> put test1 value1  # Time write (needs consensus)
> get test1         # Time read (local MVCC)

# Compare:
# - Write latency (higher - needs replication)
# - Read latency (lower - no consensus needed)
```

## 🤔 Understanding the Design

**Q: Why do writes go to the leader but reads can go to any node?**
A: Writes need consensus (coordination), but reads can use MVCC versions locally.

**Q: What happens if the leader crashes?**
A: Remaining nodes detect timeout and elect a new leader automatically.

**Q: How does MVCC prevent conflicts?**
A: Each write creates a new version instead of overwriting, so readers always see consistent data.

**Q: Why use both Raft and MVCC?**
A: Raft provides consensus and replication, MVCC provides concurrent access without locking.

**Q: Can I run nodes on different machines?**
A: Yes! Use Main.java with appropriate host:port addresses for distributed deployment.

## 🔧 Configuration Options

### Gradle Tasks
- `./gradlew runCluster` - Start a configurable cluster
- `./gradlew runClient` - Start interactive client
- `./gradlew build` - Build the project
- `./gradlew clean` - Clean build artifacts
- `./gradlew --stop` - Stop Gradle daemons

### Cluster Configuration
The cluster supports 1-20 nodes with configurable ports:
```bash
./gradlew runCluster --args="<nodeCount> <basePort>"
# Examples:
./gradlew runCluster --args="3"         # 3 nodes on ports 5001-5003
./gradlew runCluster --args="5 7000"    # 5 nodes on ports 7001-7005
```

## 📚 Further Reading

- [Raft Paper](https://raft.github.io/raft.pdf) - The original Raft consensus algorithm
- [Raft Visualization](http://thesecretlivesofdata.com/raft/) - Interactive Raft explanation
- [MVCC in PostgreSQL](https://www.postgresql.org/docs/current/mvcc.html) - Real-world MVCC
- [Distributed Systems for Fun and Profit](http://book.mixu.net/distsys/) - Excellent introduction

## 🎉 What You've Learned

After working with this project, you understand:
- **Consensus**: How distributed nodes agree on shared state
- **Consistency**: How data stays synchronized across replicas
- **Concurrency**: How to handle multiple operations efficiently
- **Fault Tolerance**: How systems survive node failures
- **Leader Election**: How distributed systems choose coordinators
- **Snapshot Isolation**: How databases provide consistent views

This is the foundation of many real distributed systems including etcd, Consul, CockroachDB, and TiDB!