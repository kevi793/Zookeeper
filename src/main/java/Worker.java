
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;


public class Worker implements Watcher
{
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private ZooKeeper zooKeeper;
    private String connectionString;
    private final Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    private static final int SESSION_TIMEOUT_IN_MS = 15000;
    private String status;

    public Worker(String connectionString) {
        this.connectionString = connectionString;
    }

    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent.toString());
    }

    private void startZk() throws IOException {
        this.zooKeeper = new ZooKeeper(this.connectionString, SESSION_TIMEOUT_IN_MS, this);
    }

    private void stopZk() throws InterruptedException {
        this.zooKeeper.close();
    }

    private void register() {
        zooKeeper.create("/workers/worker-" + this.serverId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null);
    }

    private AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)) {
                case OK:
                    LOG.info("Registered successfully " + serverId);
                    break;
                case NODEEXISTS:
                    LOG.warn("Node already registered " + serverId);
                    break;
                case CONNECTIONLOSS:
                    register();
                    return;
                default:
                    LOG.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(i), s));
            }
        }
    };

    private AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        public void processResult(int i, String s, Object o, Stat stat) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    updateStatus((String)o);
                    return;
            }
        }
    };

    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            this.zooKeeper.setData("/workers" + serverId, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Worker w = new Worker(args[0]);
        w.startZk();
        w.register();
        Thread.sleep(100000);
    }

}
