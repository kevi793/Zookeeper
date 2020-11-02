import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class Worker implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private final Random random = new Random();
    private final int SESSION_TIMEOUT = 15000;
    private final String connectionString;
    private final String serverId = Integer.toHexString(random.nextInt());
    private ZooKeeper zooKeeper;
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
                    LOG.error("Could not register worker" + KeeperException.create(rc, path));
            }
        }
    };
    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    return;
            }
        }
    };
    private volatile boolean expired;
    private volatile boolean connected;
    private String status;
    private final ChildrenCache assignedTasksCache = new ChildrenCache();
    private final ThreadPoolExecutor threadPoolExecutor;
    private final AsyncCallback.StringCallback createAssignNodeCallback = new AsyncCallback.StringCallback() {
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
                    LOG.error("Unknown error while creating assing node" + KeeperException.create(rc, path));
            }
        }
    };
    private final AsyncCallback.StringCallback taskStatusCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    LOG.info("Task status updated successfully");
                    break;
                case NODEEXISTS:
                    LOG.warn("Task status already updated");
                case CONNECTIONLOSS:
                    zooKeeper.create(path, "Done".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, taskStatusCreateCallback, null);
                    break;
                default:
                    LOG.error("Failed to create task" + KeeperException.create(rc, path));
            }
        }
    };
    private final AsyncCallback.VoidCallback taskVoidCallback = new AsyncCallback.VoidCallback() {
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    LOG.info("Successfully deleted assignment of task");
                    break;
                case CONNECTIONLOSS:
                    zooKeeper.delete(path, -1, taskVoidCallback, null);
                    break;
                default:
                    LOG.error("Failed to delete task assignment: " + KeeperException.create(rc, path));
            }
        }
    }
    private final AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] bytes, final Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    threadPoolExecutor.execute(new Runnable() {

                        byte[] data;
                        Object ctx;

                        public Runnable init(byte[] data, Object ctx) {
                            this.data = data;
                            this.ctx = ctx;
                        }

                        public void run() {
                            LOG.info("Executing the task");
                            zooKeeper.create("/status/" + ctx, "Done".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, taskStatusCreateCallback, status);
                            zooKeeper.delete("/assign/worker-" + serverId + "/" + ctx, -1, taskVoidCallback, null);
                        }
                    }.init(bytes, ctx));
                    break;
                case CONNECTIONLOSS:
                    zooKeeper.getData(path, false, taskDataCallback, ctx);
                    break;
                default:
                    LOG.error("Failed to get task data: " + KeeperException.create(rc, path));
            }
        }
    };
    AsyncCallback.ChildrenCallback getTasksCallback = new AsyncCallback.ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> list) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    if (list != null) {
                        threadPoolExecutor.execute(new Runnable() {

                            List<String> children;
                            DataCallback cb;

                            public Runnable init(List<String> children, DataCallback cb) {
                                this.children = children;
                                this.cb = cb;
                            }

                            public void run() {
                                if (children == null) {
                                    return;
                                }

                                LOG.info("Looping into tasks");
                                setStatus("Working");
                                for (String task :
                                        children) {
                                    LOG.trace("New task: {}", task);
                                    zooKeeper.getData("/assign/worker-" + serverId + "/" + task,
                                            false,
                                            cb, task);
                                }
                            }
                        }.init(assignedTasksCache.addedAndSet(list), taskDataCallback));
                    }
                    break;
                case CONNECTIONLOSS:
                    LOG.warn("Fetching tasks again.");
                    getTasks();
                    break;
                default:
                    LOG.error("Error fetching tasks." + KeeperException.create(rc, path));
            }
        }
    };
    Watcher newTaskWatcher = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                getTasks();
                return;
            }
        }
    };

    public Worker(String connectionString) {
        this.connectionString = connectionString;
        this.threadPoolExecutor = new ThreadPoolExecutor(1, 1, 1000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(200));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Worker worker = new Worker(args[0]);
        worker.startZk();

        while (!worker.isConnected()) {
            Thread.sleep(100);
        }

        worker.bootstrap();

        worker.register();

        worker.getTasks();

        while (!worker.isExpired()) {
            Thread.sleep(1000);
        }
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

    public void register() {
        this.zooKeeper.create("/workers/" + serverId, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
    }

    public void bootstrap() {
        this.createAssignNode();
    }

    private void createAssignNode() {
        this.zooKeeper.create("/assign/worker-" + serverId, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createAssignNodeCallback, null);
    }

    public void setStatus(String status) {
        this.status = status;
        this.updateStatus(status);
    }

    private void updateStatus(String status) {
        this.zooKeeper.setData("/workers/" + serverId, status.getBytes(), -1, statusUpdateCallback, status);
    }

    private void getTasks() {
        this.zooKeeper.getChildren("/assign/worker-" + serverId, newTaskWatcher, getTasksCallback, null);
    }

}
