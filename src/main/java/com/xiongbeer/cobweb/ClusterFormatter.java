package com.xiongbeer.cobweb;

import com.xiongbeer.cobweb.conf.ZnodeInfo;
import com.xiongbeer.cobweb.saver.dfs.HDFSManager;
import com.xiongbeer.cobweb.utils.Color;
import com.xiongbeer.cobweb.utils.InitLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * Created by shaoxiong on 17-4-9.
 */
public class ClusterFormatter implements Watcher {
    private static ClusterFormatter clusterFormatter;

    private static Configuration configuration = Configuration.INSTANCE;

    private ZooKeeper client;

    private HDFSManager hdfsManager;

    /**
     * 初始化znode基础设置，生成3个永久的znode
     * /
     * |--- cobweb
     * |--- wvTasks
     * |--- wvWorkers
     * |--- wvManagers
     */
    public void initZK() throws KeeperException, InterruptedException {
        try {
            createParent(ZnodeInfo.WORKERS_PATH);
            createParent(ZnodeInfo.MANAGERS_PATH);
            createParent(ZnodeInfo.TASKS_PATH);
        } catch (KeeperException.NodeExistsException e) {
            //pass
        }
    }

    /**
     * 初始化hdfs的目录树
     * /
     * |--- cobweb
     * |---bloom
     * |---tasks
     * |---newurls
     * |---waitingtasks
     * |---finishedtasks
     *
     * @throws IOException
     */
    public void initHDFS() throws IOException {
        hdfsManager.mkdirs(configuration.BLOOM_BACKUP_PATH);
        hdfsManager.mkdirs(configuration.NEW_TASKS_URLS);
        hdfsManager.mkdirs(configuration.WAITING_TASKS_URLS);
        hdfsManager.mkdirs(configuration.FINISHED_TASKS_URLS);
    }

    /**
     * 强制初始化ZooKeeper(会清除原来存在的节点)
     */
    public void formatZK() throws KeeperException, InterruptedException {
        deleteParent(ZnodeInfo.WORKERS_PATH);
        deleteParent(ZnodeInfo.MANAGERS_PATH);
        deleteParent(ZnodeInfo.TASKS_PATH);
        initZK();
    }

    /**
     * 强制初始化hdfs的目录树(会清除原来存在的文件)
     *
     * @throws IOException
     */
    public void formatHDFS() throws IOException {
        hdfsManager.delete(configuration.BLOOM_BACKUP_PATH, true);
        hdfsManager.delete(configuration.NEW_TASKS_URLS, true);
        hdfsManager.delete(configuration.WAITING_TASKS_URLS, true);
        hdfsManager.delete(configuration.FINISHED_TASKS_URLS, true);
        initHDFS();
    }

    public static synchronized ClusterFormatter getInstance() {
        if (clusterFormatter == null) {
            clusterFormatter = new ClusterFormatter();
        }
        return clusterFormatter;
    }

    private void createParent(String path)
            throws KeeperException, InterruptedException {
        client.create(path, "".getBytes(),
                OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void deleteParent(String path)
            throws KeeperException, InterruptedException {
        client.delete(path, -1);
    }

    private ClusterFormatter() {
        try {
            client = new ZooKeeper(configuration.ZK_CONNECT_STRING
                    , configuration.ZK_SESSION_TIMEOUT, this);
            hdfsManager = new HDFSManager(configuration.HDFS_SYSTEM_CONF
                    , configuration.HDFS_SYSTEM_PATH);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        InitLogger.init();
        try {
            ClusterFormatter formatter = ClusterFormatter.getInstance();
            if (args.length > 0 && args[0].equals("-f")) {
                System.out.println(Color.error("delete all old setting and  initialize?(y/n)"));
                char choice = (char) System.in.read();
                if (choice == 'y' || choice == 'Y') {
                    System.out.println("Format Zookeeper...");
                    formatter.formatZK();
                    System.out.println("Format HDFS...");
                    formatter.formatHDFS();
                    System.out.println("Done.");
                }
            } else {
                System.out.println("Init Zookeeper...");
                formatter.initZK();
                System.out.println("Init HDFS...");
                formatter.initHDFS();
                System.out.println("Done.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
    }
}
