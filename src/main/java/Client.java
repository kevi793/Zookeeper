import org.apache.zookeeper.*;

import java.io.IOException;

public class Client implements Watcher {

    private String connectionString;
    private ZooKeeper zooKeeper;
    private static final int SESSION_TIMEOUT_IN_MS = 15000;

    public Client(String connectionString) {
        this.connectionString = connectionString;
    }

    private void startZk() throws IOException {
        this.zooKeeper = new ZooKeeper(this.connectionString, SESSION_TIMEOUT_IN_MS, this);
    }

    private String queueCommand(String command) throws Exception {

        while (true) {
            try {
                String name = this.zooKeeper.create("/tasks/task-", command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (KeeperException.NodeExistsException ex) {
                throw new Exception(command + " already appears to be running");
            }
            catch (KeeperException.ConnectionLossException ex) {

            }
        }
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        Client c = new Client(args[0]);
        c.startZk();
        String name = c.queueCommand(args[1]);
        System.out.println("Created " + name);
    }
}
