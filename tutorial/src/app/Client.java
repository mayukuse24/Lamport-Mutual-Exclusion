package app;

import java.net.*;
import java.security.InvalidParameterException;
import java.io.*;
import java.util.*;
import java.time.*;

public class Client extends Node {
    List<Node> serverList = new ArrayList<Node>();

    public Client(String Id) {
        super(Id);
    }

    public void loadConfig(String fileName) {
        try {
            BufferedReader inputBuffer = new BufferedReader(new FileReader(fileName));
            String line;
            String[] params;

            System.out.println("Loading servers from config file");

            while ((line = inputBuffer.readLine()) != null) {
                params = line.split(" ");

                System.out.println(String.format("Found server %s, ip=%s, port=%s", params[0], params[1], params[2]));

                this.serverList.add(new Node(params[0], params[1], Integer.parseInt(params[2])));
            }

            inputBuffer.close();
        }
        catch (Exception e) {
            System.out.println(String.format("Could not load config from file: %s", fileName));
        }  
    }

    public static void main(String[] args) throws Exception {
        PrintWriter writer;
        BufferedReader reader;
        String appendRequest;
        String[] fileList = {"f1", "f2", "f3", "f4"};
        int totalMsgs = 10;

        Random rand = new Random();

        if (args.length < 1) {
            throw new InvalidParameterException("Incorrect number of parameters for program");
        }
        
        Client client = new Client(args[0]);

        if (args.length == 2) {
            totalMsgs = Integer.parseInt(args[1]);
        }

        Instant instant = Instant.now();

        System.out.println(String.format("Client %s starts at time: %s", client.id, instant.toEpochMilli()));

        // Get list of available file servers from config.txt file TODO: remove hard coded values
        client.loadConfig("config.txt");

        Socket echoSocket = null;

        for (int msgNo = 1; msgNo <= totalMsgs; msgNo++) {
            Thread.sleep(rand.nextInt(1000)); // Sleep for atmost 1 second

            // Select random server for sending append request
            int serverIndex = rand.nextInt(client.serverList.size());
            Node selectedServer = client.serverList.get(serverIndex);

            echoSocket = new Socket(selectedServer.ip, selectedServer.port);

            writer = new PrintWriter(echoSocket.getOutputStream(), true);
    
            reader = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));

            // Pick a random file to append message
            String message = String.format("client %s message %s -- server %s", client.id, msgNo, selectedServer.id);
            String fileName = fileList[rand.nextInt(fileList.length)];

            instant = Instant.now();

            // Identify as a client
            writer.println(String.format("client:%s:%s:%s", client.id, fileName, instant.toEpochMilli()));

            appendRequest = String.format(
                "REQ:%s:%s:%s:%s:%s",
                client.id,
                selectedServer.id,
                message,
                fileName,
                instant.toEpochMilli()
            );

            // Log client request
            System.out.println(
                String.format("client %s requests: %s for file %s at time: %s",
                    client.id,
                    message,
                    fileName,
                    instant.toEpochMilli()
                )
            );

            writer.println(appendRequest);              

            String response = reader.readLine();

            if (response.equals("ACK")) {
                System.out.println(
                    String.format("%s receives a successful ack from %s", client.id, selectedServer.id)
                );
            }
            else {
                System.out.println(String.format("%s receives a failure from %s: %s", client.id, selectedServer.id, response));
            }
            echoSocket.close();
        }

        System.out.println(String.format("client %s gracefully shuts down", client.id));        
    }
}