import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class Master implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    private static final int SESSION_TIMEOUT = 15000;
    private final static String MASTER_PATH = "/master";

    private final String connectionString;
    private final Random random = new Random(this.hashCode());
    private final String serverId = Integer.toHexString(random.nextInt());
    private ZooKeeper zooKeeper;
    private volatile MasterStates masterState = MasterStates.RUNNING;

    private volatile boolean connected = false;
    private volatile boolean expired = false;

    /**
     * Creates a new Master instance.
     *
     * @param connectionString The connection string to connect to ZK ensemble.
     */
    public Master(String connectionString) {
        this.connectionString = connectionString;
    }

    /**
     * Creates a new zookeeper session.
     *
     * @throws IOException
     */
    private void startZk() throws IOException {
        this.zooKeeper = new ZooKeeper(this.connectionString, SESSION_TIMEOUT, this);
    }

    /**
     * Closes the zookeeper session.
     * @throws InterruptedException
     */
    private void stopZk() throws InterruptedException {
        this.zooKeeper.close();
    }

    public void bootstrap() {
        this.createParent("/workers", new byte[0]);
        this.createParent("/tasks", new byte[0]);
        this.createParent("/assign", new byte[0]);
        this.createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        this.zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    LOG.info("Parent created");
                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already exists");
                    break;
                case CONNECTIONLOSS:
                    createParent(path, (byte[])ctx);
                    break;
                default:
                    LOG.error("Something went wrong ", KeeperException.create(rc, path));
            }
        }
    };

    boolean isConnected() {
        return this.connected;
    }

    boolean isExpired() {
        return this.expired;
    }

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    masterState = MasterStates.ELECTED;
                    takeLeadership();
                    break;
                case NODEEXISTS:
                    masterState = MasterStates.NOTELECTED;
                    masterExists();
                    break;
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                default:
                    masterState = MasterStates.NOTELECTED;
                    LOG.error("Something went wrong when running for master", KeeperException.create(rc, path));
            }

            LOG.info("I am " + (masterState == MasterStates.ELECTED ? "" : "not ") + "the master " + serverId);
        }
    };

    Watcher masterExistsWatcher = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                assert MASTER_PATH.equals(watchedEvent.getPath());
                runForMaster();
            }
        }
    };

    void masterExists() {
        this.zooKeeper.exists(MASTER_PATH, masterExistsWatcher, masterExistsCallback, null);
    }

    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    break;
                case NONODE:
                    masterState = MasterStates.RUNNING;
                    runForMaster();
                    LOG.info("Looks like previous master is gone");
                    break;
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

    AsyncCallback.DataCallback checkMasterCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    if (serverId.equals(new String(bytes))) {
                        masterState = MasterStates.ELECTED;
                        takeLeadership();
                    }
                    else {
                        masterState = MasterStates.NOTELECTED;
                        masterExists();
                    }
                    break;
                case NONODE:
                    runForMaster();
                    break;
                case CONNECTIONLOSS:
                    checkMaster();
                default:
                    LOG.error("Error when reading data.", KeeperException.create(rc, path));
            }
        }
    };

    void checkMaster() {
        this.zooKeeper.getData(MASTER_PATH, false, checkMasterCallback, null);
    }

    void takeLeadership() {
        LOG.info("Going for list of workers");

    }

    public void runForMaster() {
        this.zooKeeper.create(MASTER_PATH,
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null
                );
    }

    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            LOG.info("Workers change event triggered");

            if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/workers".equals(watchedEvent.getPath());
                getWorkers();
            }

        }
    };

    AsyncCallback.ChildrenCallback getWorkersCallback = new AsyncCallback.ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> list) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    LOG.info("Successfully got list of children");
//                    reassignAndSet(list);
                    break;
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                default:
                    LOG.error("Get children failed", KeeperException.create(rc, path));
            }
        }
    };

    void getWorkers() {
        this.zooKeeper.getChildren("/workers",
                workersChangeWatcher,
                getWorkersCallback,
                null);
    }

    void reassignAndSet() {

    }

    /**
     * This method implements the process method of Watcher interface.
     * We use this method to deal with different states of a session.
     *
     * @param watchedEvent A new event to be processed.
     */
    public void process(WatchedEvent watchedEvent) {
        LOG.info("Processing Event: " + watchedEvent.toString());
        if (watchedEvent.getType() == Event.EventType.None) {
            switch (watchedEvent.getState()) {
                case SyncConnected:
                    this.connected = true;
                    break;
                case Expired:
                    this.connected = false;
                    this.expired = true;
                    LOG.error("Session Expired");
                case Disconnected:
                    this.connected = false;
                    break;
                default:
                    break;
            }
        }
    }
}
