package app;

import java.net.*;
import java.io.*;
import java.util.*;
import java.security.*;
import java.time.Instant;
import java.util.concurrent.*;

import javax.naming.NameNotFoundException;

public class Server extends Node {
    public static long externalTime;
    List<Node> serverList = new ArrayList<Node>();
    Map<String, PriorityBlockingQueue<Task>> fileToTaskQueue =
        new ConcurrentHashMap<String, PriorityBlockingQueue<Task>>(Node.fileList.length);

    public Server(String Id, String Ip, int P) {
        super(Id, Ip, P);

        for (String fileName : Node.fileList) {
            this.fileToTaskQueue.put(fileName, new PriorityBlockingQueue<Task>());
        }
    }

    public void loadConfig(String fileName) throws FileNotFoundException, IOException {
        BufferedReader inputBuffer = new BufferedReader(new FileReader(fileName));
        String line;
        String[] params;

        System.out.println("Loading servers from config file");

        while ((line = inputBuffer.readLine()) != null) {
            params = line.split(" ");

            if (!params[0].equals(this.id)) { // Skip adding itself to the server list
                System.out.println(String.format("Found server %s, ip=%s, port=%s", params[0], params[1], params[2]));

                this.serverList.add(new Node(params[0], params[1], Integer.parseInt(params[2])));
            }
        }

        inputBuffer.close();
    }

    public Node getNodeFromId(String nodeId) {
        Node snode = null;

        for (Node serverNode : this.serverList) {
            if (serverNode.id.equals(nodeId)) {
                snode = serverNode;
            }
        }

        return snode;
    }

    public static synchronized long getLogicalTimestamp() {
        return Long.max(Server.externalTime, Instant.now().toEpochMilli());
    }

    public static synchronized void updateLogicalTimestamp(long time) {
        Server.externalTime = Long.max(Server.externalTime, time + 1);
    }

    public Event createEvent(Node serverNode, Task task) {
        int seqNo;
        long eventTimestamp;

        // The synchronized should generate unique seqNo and in combination with timestamp
        synchronized(serverNode.fileToSentLock.get(task.fileName)) {
            // Get timestamp for the message and the task
            eventTimestamp = Server.getLogicalTimestamp();

            seqNo = serverNode.fileToSentSeq.getOrDefault(task.fileName, 0);

            serverNode.fileToSentSeq.put(task.fileName, seqNo + 1);
        }

        // Event associated with a Task. TODO: expand on Event concept
        return new Event(eventTimestamp, seqNo);
    }

    public Event createClusterEvent(List<Node> nodesInCluster, Task task) {
        int seqNo;
        long eventTimestamp = Server.getLogicalTimestamp();
        List<Integer> seqNos = new ArrayList<Integer>();

        for (Node serverNode : nodesInCluster) {
            // The synchronized should generate different seqNo for all servers to keep coupled with timestamp
            synchronized(serverNode.fileToSentLock.get(task.fileName)) {
                // Get timestamp for the message and the task
                eventTimestamp = Server.getLogicalTimestamp();
            
                seqNo = serverNode.fileToSentSeq.getOrDefault(task.fileName, 0);

                seqNos.add(seqNo);

                serverNode.fileToSentSeq.put(task.fileName, seqNo + 1);    
            }
        }

        // Event associated with a Task. TODO: expand on Event concept
        return new Event(eventTimestamp, seqNos); 
    }

    public void broadcastToCluster(List<Channel> serverChannels, List<Integer> seqNos, String message) throws IOException {
        for (int serverIndex = 0; serverIndex < this.serverList.size(); serverIndex++) {
            this.serverList.get(serverIndex)
                .send(
                    serverChannels.get(serverIndex),
                    seqNos.get(serverIndex),
                    message
                );
        }
    }

    public void broadcastConfirmation(List<Channel> serverChannels, String fileName) throws IOException, Exception {
        String response;

        for (int serverIndex = 0; serverIndex < this.serverList.size(); serverIndex++) {
            response = this.serverList.get(serverIndex).receive(serverChannels.get(serverIndex), fileName);

            if (!response.equals("ACK")) {
                throw new Exception(String.format("Server responded with error when requesting for serverId=%s",
                    this.serverList.get(serverIndex).id));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int MAX_POOL_SIZE = 7;

        if (args.length != 3) {
            throw new InvalidParameterException("Required parameters <servername> <ip> <port>");
        }

        Server selfServer = new Server(args[0], args[1], Integer.parseInt(args[2]));

        Instant instant = Instant.now();

        // Log server start
        System.out.println(String.format("Server %s starts at time: %s", selfServer.id, instant.toEpochMilli()));

        final ExecutorService service = Executors.newFixedThreadPool(MAX_POOL_SIZE);

        Future<Integer> future = null;

        ServerSocket serverSocket = new ServerSocket(selfServer.port); // Listens on all ip addresses of host

        while (true) {
            Socket clientSocket = serverSocket.accept();

            Channel clientChannel = new Channel(clientSocket);

            System.out.println(String.format("Received connection request from ip=%s, port=%s",
                clientSocket.getInetAddress(),
                clientSocket.getPort()
            ));

            System.out.println("Received new client connection");

            requestHandler callobj = new requestHandler(
                clientChannel,
                selfServer
            );

            // Call thread to handle client connection
            future = service.submit(callobj);
        }

        // System.out.println("Exiting Server");

        // serverSocket.close();
    }
}

class requestHandler implements Callable<Integer> {

    private Channel requesterChannel;
    Server owner;
    String requesterId,
        requesterType;

    public requestHandler(Channel chnl, Server own) {
        this.requesterChannel = chnl;
        this.owner = own;
    }

    private String[] parseRequest(String request) {
        return request.split(":");
    }

    public Integer call() throws IOException, FileNotFoundException {
        String requestIdentifier = this.requesterChannel.recv();

        String[] params = requestIdentifier.split(":");

        this.requesterType = params[0];
        this.requesterId = params[1];
        String fileName = params[2];

        // Update external event time based on request timestamp
        Server.updateLogicalTimestamp(Long.parseLong(params[3]));

        System.out.println(requestIdentifier);

        // Call function based on client or server
        if (this.requesterType.equals("client")) {
            System.out.println("Request from client");

            // Check file exists in expected file list
            if (!Arrays.asList(Node.fileList).contains(fileName)) {
                System.out.println(String.format("File %s does not exist in system", fileName));

                this.requesterChannel.send("ERR: file not found");

                return 0;
            }

            try {
                this.clientHandler();

                System.out.println(String.format("server %s sends a successful ack to client %s", this.owner.id, this.requesterId));

                this.requesterChannel.send("ACK");
            }
            catch (Exception e) {
                System.out.println(String.format("Error while handling client request: %s", e));

                this.requesterChannel.send("ERR");

                return 0;
            }
            
        }
        else if (this.requesterType.equals("server")) {
            System.out.println("Request from server");

            try {
                this.serverHandler(fileName);
            }
            catch (Exception e) {
                System.out.println(String.format("Failed to handle request from server %s: %s", this.requesterId, e));

                return 0;
            }
        }

        return 1;
    }

    private void clientHandler() throws IOException, UnknownHostException, InterruptedException, Exception {
        String taskMessage;
        Event clusterEvent;
        List<Channel> serverChannels = new ArrayList<Channel>();

        // BufferedReader reader = new BufferedReader(new InputStreamReader(this.requesterSocket.getInputStream()));

        System.out.println("Reading request from client");

        String clientRequest = this.requesterChannel.recv();

        System.out.println("Read request from client");

        // Parse request to obtain required parameters for task
        String[] requestParams = this.parseRequest(clientRequest);

        System.out.println(
            String.format("server %s receives: %s for file %s at time: %s",
                this.owner.id,
                requestParams[3], // message
                requestParams[4], // file
                Server.getLogicalTimestamp()
            )
        );

        // Create task with message to be appended to file
        Task task = new Task(this.owner.id, requestParams[4], requestParams[3]);

        for (Node server : this.owner.serverList) {
            Channel chnl = new Channel(server.ip, server.port);

            // Identify as a server. TODO: change lock structure and remove filename later
            chnl.send(String.format("server:%s:%s", this.owner.id, task.fileName));

            serverChannels.add(chnl);
        }
        
        // Generate seqNos and attach timestamp to task.
        clusterEvent = this.owner.createClusterEvent(this.owner.serverList, task);

        // Attach event timestamp to task
        task.timestamp = clusterEvent.timestamp;
        
        // Add task to specific resource queue prioritized by timestamp
        this.owner.fileToTaskQueue.get(task.fileName).add(task);

        // Create request for replica servers in cluster
        taskMessage = String.format("REQ:%s:%s:%s:%s:%s",
            this.requesterId,
            task.ownerId,
            task.message,
            task.fileName,
            clusterEvent.timestamp // Needs to reflect time related to sequence no. (not necessarily task time)
        );

        System.out.println(String.format("Sending %s", taskMessage));

        // Send REQ for task to servers
        this.owner.broadcastToCluster(serverChannels, clusterEvent.sequenceNos, taskMessage);

        // Wait for ACK from everyone before processing task
        this.owner.broadcastConfirmation(serverChannels, task.fileName);

        // Wait for task to reach head of queue
        while (!this.owner.fileToTaskQueue.get(task.fileName).peek().equals(task)) {
            Thread.sleep(10); // TODO: switch to wait-notify pattern
        }

        // Process task. Currently in mutual exclusion zone
        task.execute();

        // Generate seqNos and attach timestamp to task
        clusterEvent = this.owner.createClusterEvent(this.owner.serverList, task);

        // Create release for replica servers in cluster
        taskMessage = String.format("REL:%s:%s:%s:%s:%s",
            this.requesterId,
            task.ownerId,
            task.message,
            task.fileName,
            clusterEvent.timestamp
        );

        System.out.println(String.format("Sending %s", taskMessage));

        // Send release to all servers in cluster
        this.owner.broadcastToCluster(serverChannels, clusterEvent.sequenceNos, taskMessage);

        // Wait for ACK from everyone
        this.owner.broadcastConfirmation(serverChannels, task.fileName);

        // Remove task from queue
        if (!this.owner.fileToTaskQueue.get(task.fileName).poll().equals(task)) {
            throw new Exception(String.format("Incorrect task removed from head of Queue %s", task));
        }

        // Cleanup sockets
        for (Channel chnl : serverChannels) {
            chnl.close();
        }
    }

    private void serverHandler(String fileName) throws Exception {
        Event clusterEvent;

        // Get the server information that sent the request
        Node requesterNode = this.owner.getNodeFromId(this.requesterId);

        // Get the task request
        String serverRequest = requesterNode.receive(this.requesterChannel, fileName);

        // Parse task request
        String[] requestParams = this.parseRequest(serverRequest);

        // Check if server present in config
        if (requesterNode.equals(null)) {
            throw new NameNotFoundException(String.format("Could not find server in config with id=%s", requestParams[2]));
        }

        // Create task
        Task task = new Task(requesterNode.id, fileName, requestParams[3]);

        // Set task timestamp. This timestamp depends on server that created task. Therefore use time provided in request
        task.timestamp = Long.parseLong(requestParams[5]); 

        // Add task to queue
        this.owner.fileToTaskQueue.get(task.fileName).add(task);

        // Create event containing sequence no. and timestamp
        clusterEvent = this.owner.createEvent(requesterNode, task);

        // Send ACK Response
        requesterNode.send(this.requesterChannel, clusterEvent.sequenceNos.get(0), "ACK");

        // Wait for Release message
        String releaseMessage = requesterNode.receive(this.requesterChannel, task.fileName);
        
        // Wait for task to reach head of queue
        while (!this.owner.fileToTaskQueue.get(task.fileName).peek().equals(task)) {
            Thread.sleep(10); // TODO: switch to wait-notify pattern
        }

        // Execute task
        task.execute();

        // Remove task from queue
        if (!this.owner.fileToTaskQueue.get(task.fileName).poll().equals(task)) {
            throw new Exception(String.format("Incorrect task removed from head of Queue %s", task));
        }

        // Create event containing sequence no. and timestamp
        clusterEvent = this.owner.createEvent(requesterNode, task);

        // Send ACK Response
        requesterNode.send(this.requesterChannel, clusterEvent.sequenceNos.get(0), "ACK");

        // Cleanup socket
        this.requesterChannel.close();
    }
}