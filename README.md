# Lamport-Mutual-Exclusion
An implementation of lamport's mutual exclusion algorithm that uses total ordering of events to allow concurrent file appends across distributed file servers.

## Usage

### Configuration

The config.txt file is used by the client and server to locate other servers in the cluster. Add the (correct) configuration to the config.txt file. Each line should contain <server-id> <ip> <port> and a server should be running with the same configuration. See sample config.txt as an example.

### Compile

```
javac tutorial/src/app/\*.java -d .
```

### Run server

```
java app.Server <server-id> <listen-ip> <listen-port>

Ex: java app.Server S1 0.0.0.0 10000
```

### Run Client

```
java app.Client <client-id> <total-tasks>
 
Ex: java app.Client C1 100
```
