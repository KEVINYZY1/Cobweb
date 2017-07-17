package com.xiongbeer.webveins.check;

import com.xiongbeer.webveins.Configuration;
import com.xiongbeer.webveins.WebVeinsMain;
import com.xiongbeer.webveins.ZnodeInfo;
import com.xiongbeer.webveins.saver.DFSManager;
import com.xiongbeer.webveins.saver.HDFSManager;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;

/**
 * Created by shaoxiong on 17-5-6.
 */
public class SelfTest {
    private static final Logger logger = LoggerFactory.getLogger(SelfTest.class);

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
            RetryPolicy retryPolicy =
                    new ExponentialBackoffRetry(Configuration.ZK_RETRY_INTERVAL, Configuration.ZK_RETRY_TIMES);
            client = CuratorFrameworkFactory
                    .newClient(Configuration.ZK_CONNECT_STRING
                            , Configuration.ZK_SESSION_TIMEOUT, Configuration.ZK_INIT_TIMEOUT, retryPolicy);
            client.start();
            client.checkExists().forPath(ZnodeInfo.TASKS_PATH);
            client.checkExists().forPath(ZnodeInfo.MANAGERS_PATH);
            client.checkExists().forPath(ZnodeInfo.WORKERS_PATH);
        } catch (Throwable e) {
            client = null;
            logger.error(e.getMessage());
        }
        return Optional.of(client).get();
    }

    /**
     * 检查HDFS的连接状态和它的目录树
     *
     * @return
     */
    public static DFSManager checkAndGetDFS() {
        DFSManager hdfsManager;
        try {
            hdfsManager = new HDFSManager(Configuration.HDFS_SYSTEM_CONF, Configuration.HDFS_SYSTEM_PATH);
            hdfsManager.exist(Configuration.BLOOM_BACKUP_PATH);
            hdfsManager.exist(Configuration.FINISHED_TASKS_URLS);
            hdfsManager.exist(Configuration.WAITING_TASKS_URLS);
            hdfsManager.exist(Configuration.NEW_TASKS_URLS);
        } catch (Throwable e) {
            hdfsManager = null;
            logger.error(e.getMessage());
        }
        return Optional.of(hdfsManager).get();
    }
}
