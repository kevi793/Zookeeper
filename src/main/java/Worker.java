import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;


public class Worker implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private final Random random = new Random();
    private final int SESSION_TIMEOUT = 15000;
    private final String connectionString;
    private final String serverId = Integer.toHexString(random.nextInt());
    private ZooKeeper zooKeeper;
    private volatile boolean expired;
    private volatile boolean connected;

    public Worker(String connectionString) {
        this.connectionString = connectionString;
    }

    void startZk() throws IOException {
        this.zooKeeper = new ZooKeeper(this.connectionString, SESSION_TIMEOUT, this);
    }

    void stopZk() throws InterruptedException {
        this.zooKeeper.close();
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.None) {
            switch (watchedEvent.getState()) {
                case SyncConnected:
                    connected = true;
                    expired = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("Session expired");
                    break;
                case Disconnected:
                    connected = false;
                    break;
                default:
                    break;
            }
        }
    }

    public boolean isConnected() {
        return this.connected;
    }

    public boolean isExpired() {
        return this.expired;
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    LOG.info("Worker registered successfully.");
                    break;
                case NODEEXISTS:
                    LOG.warn("Worker already created.");
                    break;
                case CONNECTIONLOSS:
                    LOG.info("Connection lost. Trying to create again.");
                    register();
                    break;
                default:
                    LOG.error("Could not register worker", KeeperException.create(rc, path));
            }
        }
    };

    public void register() {
        this.zooKeeper.create("/workers/" + serverId, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
    }

    public void bootstrap() {
        this.createAssignNode();
    }

    private void createAssignNode() {
        this.zooKeeper.create("/assign/worker-" + serverId, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createAssignNodeCallback, null);
    }

    private AsyncCallback.StringCallback createAssignNodeCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    LOG.info("Assign node created");
                    break;
                case NODEEXISTS:
                    LOG.warn("Assign node already exists");
                    break;
                case CONNECTIONLOSS:
                    LOG.warn("Assign node failed due to connection loss. Trying again.");
                    createAssignNode();
                    break;
                default:
                    LOG.error("Unknown error while creating assing node", KeeperException.create(rc, path));
            }
        }
    }

}
