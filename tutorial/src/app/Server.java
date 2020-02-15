package app;

import java.net.*;
import java.io.*;
import java.util.*;
import java.security.*;
import java.time.Instant;
import java.util.concurrent.*;

public class Server extends Node {
    public static long externalTime;
    List<Node> serverList = new ArrayList<Node>();

    public Server(String Id, String Ip, int P) {
        super(Id, Ip, P);
    }

    public void loadConfig(String fileName) {
        try {
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
        catch (Exception e) {
            System.out.println(String.format("Could not load config from file: %s", fileName));
        }  
    }

    public static synchronized long getLogicalTimestamp() {
        return Long.max(Server.externalTime, Instant.now().toEpochMilli());
    }

    public static synchronized void updateLogicalTimestamp(long time) {
        Server.externalTime = Long.max(Server.externalTime, time);
    }

    public void createTask(String fileName, String message) {
        int seqNo;

        // TODO: the synchronized should generate different seqNo for all servers to keep coupled with timestamp
        synchronized(this.fileToSentLock.get(fileName)) {
            seqNo = fileToSentSeq.getOrDefault(fileName, 0);

            // TODO: timestamp the message and the task here

            fileToSentSeq.put(fileName, seqNo + 1);
        }
    }

    private Socket connectUntil(String ip, Integer port) throws InterruptedException {
        Socket echoSocket = null;

        while (true) {
            try {
                echoSocket = new Socket(ip, port);
                return echoSocket;
            }
            catch (Exception e) {
                System.out.println(String.format("Waiting for server at ip=%s, port=%s ...", ip, port));
                Thread.sleep(1000);
            }
        }
    }

    public static void main(String[] args) throws Exception {
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
                selfServer,
                Instant.now().toEpochMilli() // Update this to use max() local and recieved time
            );

            // Call thread to handle client connection
            future = service.submit(callobj);
        }

        // System.out.println("Exiting Server");

        // serverSocket.close();
    }
}

class requestHandler implements Callable<Integer> {

    private Socket requestSocket;
    Server owner;
    String senderId,
        message,
        senderType,
        fileName;
    long timestamp;

    public requestHandler(Socket sock, Server own, long ts) {
        this.requestSocket = sock;
        this.owner = own;
        this.timestamp = ts;
    }

    private void parseRequest(String request) {
        String[] params = request.split(":");

        this.senderId = params[0];
        this.message = params[1];
        this.timestamp = Long.parseLong(params[3]);
        this.senderType = "client";

        for (Node server : this.owner.serverList) {
            if (server.id.equals(this.senderId)) { // Checking if the request is from a file server or client
                this.senderType = "server";
            }
        }
    }

    private void clientHandler() throws Exception {
        List<Socket> serverSockets = new ArrayList<Socket>();
        List<PrintWriter> writers = new ArrayList<PrintWriter>();
        List<BufferedReader> readers = new ArrayList<BufferedReader>();

        // Create task
        // Add task to queue
        // Send REQ for task to servers
        // Wait for ACK from everyone
        // Wait for task to reach head of queue
        // Process task. Append line to file
        // Send REL
        // Wait for ACK from everyone
        // 
        for (Node server : this.owner.serverList) {
            Socket serverSocket = new Socket(server.ip, server.port);

            serverSockets.add(serverSocket);

            writers.add(new PrintWriter(serverSocket.getOutputStream(), true));

            readers.add(new BufferedReader(new InputStreamReader(serverSocket.getInputStream())));
        }

        for (PrintWriter writer : writers) {
            writer.println(String.format("REQ:%s:%s:%s:%s:%s",
                this.owner.id,
                this.message,
                "SN", // TODO: fix this
                this.fileName,
                this.timestamp
            ));
        }

        for (BufferedReader reader : readers) {
            if (!reader.readLine().equals("ACK")) {
                throw new Exception("Server responded with error when requesting"); // TODO: Which reader failed?
            }
        }

        // Enter critical section. Append message to file
        
        PrintWriter fileObj = new PrintWriter(new FileWriter(this.fileName, true));

        fileObj.println(this.message);

        fileObj.close();

        for (PrintWriter writer : writers) {
            writer.println(String.format("REL:%s:%s:%s:%s",
                this.owner.id,
                "SN", // TODO: fix this
                this.fileName,
                this.timestamp
            ));
        }

        for (BufferedReader reader : readers) {
            if (!reader.readLine().equals("ACK")) {
                throw new Exception("Server responded with error when releasing"); // TODO: Which reader failed?
            }
        }

        // Remove task from queue

        // Cleanup sockets, readers and writers

    }

    private void serverHandler() {

    }

    public Integer call() throws Exception {
        PrintWriter out = new PrintWriter(this.requestSocket.getOutputStream(), true);

        BufferedReader in = new BufferedReader(new InputStreamReader(this.requestSocket.getInputStream()));

        String recvLine;

        recvLine = in.readLine();

        System.out.println(recvLine);

        // Parse request to figure out if client or server request
        this.parseRequest(recvLine);

        // Call function based on client or server
        if (this.senderType.equals("client")) {
            System.out.println("Request from client");

            try {
                clientHandler();

                out.println("ACK");
            }
            catch (Exception e) {
                System.out.println(String.format("Error while handling client request: %s", recvLine));

                out.println("ERR");
            }
            
        }
        else if (this.senderType.equals("server")) {
            System.out.println("Request from server");

            serverHandler();
        }

        return 1;
    }
}