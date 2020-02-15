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
        System.out.println("Client Started");

        PrintWriter out;
        BufferedReader in;
        String appendRequest;
        String[] fileList = {"f1", "f2", "f3", "f4"};

        Random rand = new Random();

        if (args.length != 1) {
            throw new InvalidParameterException("Incorrect number of parameters for program");
        }
        
        Client client = new Client(args[0]);

        // Get list of available file servers from config.txt file
        client.loadConfig("config.txt");

        System.out.println(client.serverList);

        Socket echoSocket = null;

        for (int msgNo = 1; msgNo <= 10; msgNo++) {
            Thread.sleep(rand.nextInt(1000)); // Sleep for atmost 1 second

            // Select random server for sending append request
            int serverIndex = rand.nextInt(client.serverList.size());
            Node selectedServer = client.serverList.get(serverIndex);

            echoSocket = new Socket(selectedServer.ip, selectedServer.port);

            out = new PrintWriter(echoSocket.getOutputStream(), true);
    
            in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));

            // Pick a random file to append message
            String fileName = fileList[rand.nextInt(fileList.length)];

            Instant instant = Instant.now();

            appendRequest = String.format(
                "%s requests:client %s message #%s -- server %s: for file #%s at time: %s",
                client.id,
                client.id,
                msgNo,
                selectedServer.id,
                fileName,
                instant.toEpochMilli()
            );

            System.out.println(appendRequest);

            try {
                out.println(appendRequest);
            }
            catch (Exception e) {
                System.out.println(String.format("%s receives a failure from %s", client.id, selectedServer.id));
            }                 

            String response = in.readLine();

            if (response.equals("ACK")) {
                System.out.println(
                    String.format("%s receives a successful ack from %s", client.id, selectedServer.id)
                );
            }
            else {
                System.out.println(String.format("%s receives a failure from %s", client.id, selectedServer.id));
            }
            echoSocket.close();
        }
        

        System.out.println("Exiting Client");        
    }
}