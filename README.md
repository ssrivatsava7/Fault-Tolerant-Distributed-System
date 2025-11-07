# ⚙️ Fault-Tolerant Distributed Database System  

## 🧭 Overview  

This project implements a **Fault-Tolerant Distributed Database System** designed to ensure **reliability, availability, and consistency** in the presence of node crashes or network failures.  

The system uses a **replicated server architecture** with **client–server communication** and **consensus mechanisms (via GigaPaxos)** to maintain consistent state across multiple nodes.  
It demonstrates core distributed-systems concepts including **replication**, **leader election**, **failure detection**, and **transparent recovery** using Java.


## 🎯 Objectives  

- ✅ Build a distributed database that remains operational despite individual node failures.  
- ✅ Implement **replication** and **failover** mechanisms to maintain availability.  
- ✅ Use **GigaPaxos** or equivalent protocol for **consensus and consistency**.  
- ✅ Demonstrate **client–server interactions** with real-time recovery and state replication.  
- ✅ Provide a scalable framework to simulate real-world distributed storage systems like Cassandra.


## 🧩 System Architecture  
**High-Level Design Components:**

| Component | Description |
|------------|-------------|
| **Client Layer** | Manages client requests, sends read/write operations to server replicas. |
| **Server Layer** | Handles distributed storage, replication, and synchronization across nodes. |
| **Fault Tolerance Layer** | Implements heartbeat checks, leader election, and failover recovery. |
| **Configuration Layer** | Loads and manages properties (server addresses, cluster info) via `gigapaxos.properties` and `servers.properties`. |
| **Persistence Layer** | Interfaces with Cassandra-like backend or in-memory store for durable writes. |


## ⚙️ Technologies & Tools  

| Category | Tools / Libraries |
|-----------|-------------------|
| **Programming Language** | Java 11+ |
| **Distributed Framework** | GigaPaxos |
| **Database Layer** | Cassandra / custom in-memory replica |
| **Build System** | Maven |
| **Testing** | JUnit |
| **Logging** | SLF4J / Log4j2 |
| **Configuration** | Property-based node and server setup |


## 🚀 How to Run  

### 1️⃣ Clone the Repository  
```bash
git clone https://github.com/ssrivatsava7/Fault-Tolerant-Distributed-System.git
cd Fault-Tolerant-Distributed-System
```

2️⃣ Build the Project
```bash
mvn clean package
```

3️⃣ Configure the Cluster
Edit the configuration files in conf/:
-servers.properties → defines active nodes and ports.
-gigapaxos.properties → defines replication groups and Paxos roles.

4️⃣ Run the Servers
Start one or more replica servers:
```bash
java -cp target/fault-tolerant-db-main.jar server.faulttolerance.AVDBReplicatedServer
```

5️⃣ Run the Client
Execute client requests (read/write operations):
```bash
java -cp target/fault-tolerant-db-main.jar client.MyDBClient
```
Logs and recovery events will be printed to the console and log files defined in logging.properties.

📊 Sample Results
-Scenario	Behavior
-Single Node Failure - System automatically fails over to secondary replica without downtime.
-Network Partition - Client temporarily re-routes requests to available nodes.
-Write Operation - Replicated across all nodes for consistency (via Paxos consensus).
-Recovery - Failed node replays missed operations from the leader log upon restart.

📘 Design Document
For a detailed explanation of the architecture, message flow, and fault-tolerance mechanisms, see DesignDoc.pdf in the root directory.

🔮 Future Enhancements
-Implement Byzantine Fault Tolerance (BFT) for malicious failure scenarios.
-Add dynamic membership — allowing live addition/removal of nodes.
-Introduce real-time metrics dashboard for monitoring cluster state.

Optimize replication strategy for performance under high load.

