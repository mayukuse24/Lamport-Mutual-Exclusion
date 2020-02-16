package app;

import java.util.*; 

public class Task {
    long timestamp;
    String ownerId;

    public Task(long ts, String oid) {
        this.timestamp = ts;
        this.ownerId = oid;
    }

    public void execute() {
        PrintWriter fileObj = new PrintWriter(new FileWriter(this.fileName, true));

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