package com.xiongbeer.cobweb;

import com.xiongbeer.cobweb.conf.ZNodeStaticSetting;
import com.xiongbeer.cobweb.saver.dfs.HDFSManager;
import com.xiongbeer.cobweb.utils.Color;
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

    private static com.xiongbeer.cobweb.conf.Configuration configuration = com.xiongbeer.cobweb.conf.Configuration.INSTANCE;

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
            createParent(ZNodeStaticSetting.WORKERS_PATH);
            createParent(ZNodeStaticSetting.MANAGERS_PATH);
            createParent(ZNodeStaticSetting.TASKS_PATH);
            createParent(ZNodeStaticSetting.FILTERS_ROOT);
        } catch (KeeperException.NodeExistsException e) {
            // pass is ok
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
        hdfsManager.mkdirs((String) configuration.get("bloom_backup_path"));
        hdfsManager.mkdirs((String) configuration.get("new_tasks_urls"));
        hdfsManager.mkdirs((String) configuration.get("waiting_tasks_urls"));
        hdfsManager.mkdirs((String) configuration.get("finished_tasks_urls"));
    }

    /**
     * 强制初始化ZooKeeper(会清除原来存在的节点)
     */
    public void formatZK() throws KeeperException, InterruptedException {
        deleteParent(ZNodeStaticSetting.WORKERS_PATH);
        deleteParent(ZNodeStaticSetting.MANAGERS_PATH);
        deleteParent(ZNodeStaticSetting.TASKS_PATH);
        deleteParent(ZNodeStaticSetting.FILTERS_ROOT);
        initZK();
    }

    /**
     * 强制初始化hdfs的目录树(会清除原来存在的文件)
     *
     * @throws IOException
     */
    public void formatHDFS() throws IOException {
        hdfsManager.delete((String) configuration.get("bloom_backup_path"), true);
        hdfsManager.delete((String) configuration.get("new_tasks_urls"), true);
        hdfsManager.delete((String) configuration.get("waiting_tasks_urls"), true);
        hdfsManager.delete((String) configuration.get("finished_tasks_urls"), true);
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
            client = new ZooKeeper((String) configuration.get("zk_connect_string")
                    , (int) configuration.get("zk_session_timeout"), this);
            hdfsManager = new HDFSManager((org.apache.hadoop.conf.Configuration) configuration.get("hdfs_system_conf")
                    , (String) configuration.get("hdfs_system_path"));
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
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
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
    }
}