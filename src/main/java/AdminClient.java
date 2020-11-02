import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class AdminClient implements Watcher {

    private ZooKeeper zooKeeper;
    private String connectionString;
    private static final int SESSION_TIMEOUT_IN_MS = 15000;

    public AdminClient(String connectionString) {
        this.connectionString = connectionString;
    }

    private void startZk() throws IOException {
        this.zooKeeper = new ZooKeeper(connectionString, SESSION_TIMEOUT_IN_MS, this);
    }

    private void stopZk() throws InterruptedException {
        this.zooKeeper.close();
    }

    private void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte[] masterData = this.zooKeeper.getData("/master", false, stat);
        } catch (KeeperException.NoNodeException e) {
            System.out.println("No master");
        }

        System.out.println("Workers:");

        for (String w: this.zooKeeper.getChildren("/workers", false)) {
            byte[] data = this.zooKeeper.getData("/workers/" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ": " + state);
        }


        System.out.println("Tasks:");
        for (String t : this.zooKeeper.getChildren("/tasks", false)) {
            System.out.println("\t" + t);
        }
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        AdminClient adminClient = new AdminClient(args[0]);
        adminClient.startZk();
        adminClient.listState();
    }
}
