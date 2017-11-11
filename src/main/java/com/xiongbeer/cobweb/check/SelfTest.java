package com.xiongbeer.cobweb.check;

import com.xiongbeer.cobweb.conf.Configuration;
import com.xiongbeer.cobweb.conf.ZNodeStaticSetting;
import com.xiongbeer.cobweb.saver.dfs.DFSManager;
import com.xiongbeer.cobweb.saver.dfs.HDFSManager;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by shaoxiong on 17-5-6.
 */
public class SelfTest {
    private static final Logger logger = LoggerFactory.getLogger(SelfTest.class);

    private static Configuration configuration = Configuration.INSTANCE;

    /**
     * 检查某个class是否已经在运行
     *
     * @param className
     * @return
     */
    public static boolean checkRunning(String className) {
        boolean result = false;
        int counter = 0;
        try {
            Process process = Runtime.getRuntime().exec("jps");
            InputStreamReader iR = new InputStreamReader(process.getInputStream());
            BufferedReader input = new BufferedReader(iR);
            String line;
            while ((line = input.readLine()) != null) {
                if (line.matches(".*" + className)) {
                    counter++;
                    if (counter > 1) {
                        result = true;
                        break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * 检查ZooKeeper的连接状态和它的Znode目录树
     *
     * @param
     * @return
     */
    public static CuratorFramework checkAndGetZK() {
        CuratorFramework client;
        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                    (int) configuration.get("zk_retry_interval"), (int) configuration.get("zk_retry_times"));
            client = CuratorFrameworkFactory
                    .newClient((String) configuration.get("zk_connect_string")
                            , (int) configuration.get("zk_session_timeout")
                            , (int) configuration.get("zk_init_timeout"), retryPolicy);
            client.start();
            client.checkExists().forPath(ZNodeStaticSetting.TASKS_PATH);
            client.checkExists().forPath(ZNodeStaticSetting.MANAGERS_PATH);
            client.checkExists().forPath(ZNodeStaticSetting.WORKERS_PATH);
            client.checkExists().forPath(ZNodeStaticSetting.FILTERS_ROOT);
        } catch (Throwable e) {
            client = null;
            logger.error(e.getMessage());
        }
        return client;
    }

    /**
     * 检查HDFS的连接状态和它的目录树
     *
     * @return
     */
    public static DFSManager checkAndGetDFS() {
        DFSManager hdfsManager;
        try {
            hdfsManager = new HDFSManager((org.apache.hadoop.conf.Configuration) configuration.get("hdfs_system_conf")
                    , (String) configuration.get("hdfs_system_path"));
            hdfsManager.exist((String) configuration.get("bloom_backup_path"));
            hdfsManager.exist((String) configuration.get("finished_tasks_urls"));
            hdfsManager.exist((String) configuration.get("waiting_tasks_urls"));
            hdfsManager.exist((String) configuration.get("new_tasks_urls"));
        } catch (Throwable e) {
            hdfsManager = null;
            logger.error(e.getMessage());
        }
        return hdfsManager;
    }
}
