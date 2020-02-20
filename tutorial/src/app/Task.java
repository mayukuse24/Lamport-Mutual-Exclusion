package app;

import java.io.*;
import java.util.*;

public class Task {
    public long timestamp;
    String ownerId,
        fileName,
        message;

    public Task(String oid, String fname, String message) {
        this.ownerId = oid;
        this.fileName = fname;
        this.message = message;
    }

    public void execute() throws IOException {
        // TODO: handle failure when file does not exist
        PrintWriter fileObj = new PrintWriter(new FileWriter("files/" + this.fileName, true));

        fileObj.println(this.message);

        fileObj.close();
    }
};

class TaskComparator implements Comparator<Task> {
      
    // Overriding compare()method of Comparator for tasks by ascending timestamp, ties broken by ascending serverIds
    public int compare(Task t1, Task t2) { 
        if (t1.timestamp > t2.timestamp) {
            return 1;
        }
        else if (t1.timestamp < t2.timestamp) {
            return -1; 
        }
        else {
            if (t1.ownerId.compareTo(t2.ownerId) > 0) {
                return 1;
            }
            else {
                return -1;
            }
        }
    }
} 