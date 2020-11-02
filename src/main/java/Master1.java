import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

public class Master1 implements Watcher
{
    private String connectionString;
    private ZooKeeper zooKeeper;
    private static final int sessionTimeoutInMs = 15000;
    private final Random random = new Random();
    private final String serverId = Integer.toHexString(random.nextInt());
    static boolean isLeader = false;

    private static AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)) {
                case OK:
                    isLeader = true;
                    break;
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                default:
                    isLeader = false;
            }

            System.out.println("I am " + (isLeader ? "" : "not ") + "the leader");
        }
    };

    private static AsyncCallback.StringCallback checkMaster = new AsyncCallback.StringCallback() {
        public void processResult(int i, String s, Object o, String s1) {

        }
    }


    public Master1(String connectionString) {
        this.connectionString = connectionString;
    }

    private void startZk() throws IOException {
        this.zooKeeper = new ZooKeeper(connectionString, sessionTimeoutInMs, this);
    }

    private void stopZk() throws InterruptedException {
        this.zooKeeper.close();
    }

    private void runForMaster() throws InterruptedException {

        while (true) {
            try {
                this.zooKeeper
                        .create("/master",
                                serverId.getBytes(),
                                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.EPHEMERAL);
                this.isLeader = true;
                break;
            }
            catch (KeeperException.NodeExistsException ex) {
                this.isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException ex) {

            } catch (KeeperException e) {
                e.printStackTrace();
            }

            if (this.checkMaster()) {
                break;
            }
        }
    }

    private boolean checkMaster() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = this.zooKeeper.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException ex) {
                return false;
            }
            catch (Exception ex) {

            }
        }
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println("Event received by master: " + watchedEvent);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Master1 master1 = new Master1("localhost:2181");
        master1.startZk();

        master1.runForMaster();

        if (master1.isLeader) {
            System.out.println("I am the leader");
            Thread.sleep(60000);
        } else {
            System.out.println("Somebody else is the master");
        }

        master1.stopZk();
    }
}
