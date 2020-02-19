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
        synchronized(this.fileToSentLock.get(task.fileName)) {
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
        long eventTimestamp;
        List<Integer> seqNos = new ArrayList<Integer>();

        // The synchronized should generate different seqNo for all servers to keep coupled with timestamp
        synchronized(this.fileToSentLock.get(task.fileName)) {
            // Get timestamp for the message and the task
            eventTimestamp = Server.getLogicalTimestamp();

            for (Node serverNode : nodesInCluster) {
                seqNo = serverNode.fileToSentSeq.getOrDefault(task.fileName, 0);

                seqNos.add(seqNo);

                serverNode.fileToSentSeq.put(task.fileName, seqNo + 1);    
            }
        }

        // Event associated with a Task. TODO: expand on Event concept
        return new Event(eventTimestamp, seqNos); 
    }

    public void broadcastToCluster(List<Socket> serverSockets, List<Integer> seqNos, String message) throws IOException {
        for (int serverIndex = 0; serverIndex < this.serverList.size(); serverIndex++) {
            this.serverList.get(serverIndex)
                .send(
                    serverSockets.get(serverIndex),
                    seqNos.get(serverIndex),
                    message
                );
        }
    }

    public void broadcastConfirmation(List<Socket> serverSockets, String fileName) throws IOException, Exception {
        String response;

        for (int serverIndex = 0; serverIndex < this.serverList.size(); serverIndex++) {
            response = this.serverList.get(serverIndex).receive(serverSockets.get(serverIndex), fileName);

            if (!response.equals("ACK")) {
                throw new Exception(String.format("Server responded with error when requesting for serverId=%s",
                    this.serverList.get(serverIndex).id));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int MAX_POOL_SIZE = 3;

        System.out.println("Server Started");

        if (args.length != 3) {
            throw new InvalidParameterException("Required parameters <servername> <ip> <port>");
        }

        Server selfServer = new Server(args[0], args[1], Integer.parseInt(args[2]));

        final ExecutorService service = Executors.newFixedThreadPool(MAX_POOL_SIZE);

        Future<Integer> future = null;

        ServerSocket serverSocket = new ServerSocket(selfServer.port); // Listens on all ip addresses of host

        while (true) {
            Socket clientSocket = serverSocket.accept();

            System.out.println(String.format("Received connection request from ip=%s, port=%s",
                clientSocket.getInetAddress(),
                clientSocket.getPort()
            ));

            System.out.println("Received new client connection");

            requestHandler callobj = new requestHandler(
                clientSocket,
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

    private Socket requesterSocket;
    Server owner;
    String requesterId,
        requesterType;

    public requestHandler(Socket sock, Server own) {
        this.requesterSocket = sock;
        this.owner = own;
    }

    private String[] parseRequest(String request) {
        return request.split(":");
    }

    public Integer call() throws IOException, FileNotFoundException {
        PrintWriter writer = new PrintWriter(this.requesterSocket.getOutputStream(), true);

        BufferedReader reader = new BufferedReader(new InputStreamReader(this.requesterSocket.getInputStream()));

        String requestIdentifier = reader.readLine();

        String[] params = requestIdentifier.split(":");

        this.requesterType = params[0];
        this.requesterId = params[1];
        String fileName = params[2];

        // Update external event time based on request timestamp
        Server.updateLogicalTimestamp(Long.parseLong(params[3]));

        System.out.println(this.requesterType);

        // Call function based on client or server
        if (this.requesterType.equals("client")) {
            System.out.println("Request from client");

            // Check file exists in expected file list
            if (!Arrays.asList(Node.fileList).contains(fileName)) {
                System.out.println(String.format("File %s does not exist in system", fileName));

                writer.println("ERR: file not found");

                return 0;
            }

            try {
                this.clientHandler();

                writer.println("ACK");
            }
            catch (Exception e) {
                System.out.println(String.format("Error while handling client request: %s", requestIdentifier));

                writer.println("ERR");

                return 0;
            }
            
        }
        else if (this.requesterType.equals("server")) {
            System.out.println("Request from server");

            try {
                this.serverHandler(fileName);
            }
            catch (Exception e) {
                System.out.println(String.format("Failed to handle request from server: %s", this.requesterId));

                return 0;
            }
        }

        return 1;
    }

    private void clientHandler() throws IOException, UnknownHostException, InterruptedException, Exception {
        String taskMessage;
        Event clusterEvent;
        List<Socket> serverSockets = new ArrayList<Socket>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(this.requesterSocket.getInputStream()));

        String clientRequest = reader.readLine();

        // Parse request to obtain required parameters for task
        String[] requestParams = this.parseRequest(clientRequest);

        // Create task with message to be appended to file
        Task task = new Task(this.owner.id, requestParams[4], requestParams[3]);

        for (Node server : this.owner.serverList) {
            Socket serverSocket = new Socket(server.ip, server.port);

            // TODO: shift this to Node class later (connect and listen methods)
            PrintWriter writer = new PrintWriter(serverSocket.getOutputStream(), true);

            // Identify as a server. TODO: change lock structure and remove filename later
            writer.println(String.format("server:%s:%s", this.owner.id, task.fileName));

            serverSockets.add(serverSocket);
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

        // Send REQ for task to servers
        this.owner.broadcastToCluster(serverSockets, clusterEvent.sequenceNos, taskMessage);

        // Wait for ACK from everyone before processing task
        this.owner.broadcastConfirmation(serverSockets, task.fileName);

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

        // Send release to all servers in cluster
        this.owner.broadcastToCluster(serverSockets, clusterEvent.sequenceNos, taskMessage);

        // Wait for ACK from everyone
        this.owner.broadcastConfirmation(serverSockets, task.fileName);

        // Remove task from queue
        if (!this.owner.fileToTaskQueue.get(task.fileName).poll().equals(task)) {
            throw new Exception(String.format("Incorrect task removed from head of Queue %s", task));
        }

        // Cleanup sockets
        for (Socket serverSocket : serverSockets) {
            serverSocket.close();
        }
    }

    private void serverHandler(String fileName) throws Exception {
        Event clusterEvent;

        // Get the server information that sent the request
        Node requesterNode = this.owner.getNodeFromId(this.requesterId);

        // Get the task request
        String serverRequest = requesterNode.receive(this.requesterSocket, fileName);

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
        requesterNode.send(this.requesterSocket, clusterEvent.sequenceNos.get(0), "ACK");

        // Wait for Release message
        String releaseMessage = requesterNode.receive(this.requesterSocket, task.fileName);
        
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
        requesterNode.send(this.requesterSocket, clusterEvent.sequenceNos.get(0), "ACK");

        // Cleanup socket
        this.requesterSocket.close();
    }
}