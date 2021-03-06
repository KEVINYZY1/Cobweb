package com.xiongbeer.cobweb.api.job;

import com.xiongbeer.cobweb.Configuration;
import com.xiongbeer.cobweb.conf.ZNodeStaticSetting;
import com.xiongbeer.cobweb.api.SimpleJob;
import com.xiongbeer.cobweb.exception.CobwebRuntimeException;
import com.xiongbeer.cobweb.saver.dfs.DFSManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by shaoxiong on 17-5-12.
 */
public class TaskJob implements SimpleJob {
    private static int MAX_OUTPUT_INFO = 10;

    private static Configuration configuration = Configuration.INSTANCE;

    private CuratorFramework client;

    private DFSManager dfsManager;

    public TaskJob(CuratorFramework client, DFSManager dfsManager) {
        this.client = client;
        this.dfsManager = dfsManager;
    }

    public String removeTasks(String regex) {
        String separator = System.getProperty("line.separator");
        StringBuilder builder = new StringBuilder(separator);
        int counter = 0;
        try {
            Pattern pattern = Pattern.compile(regex);
            for (String task : getTasksName()) {
                Matcher matcher = pattern.matcher(task);
                while (matcher.find()) {
                    removeFromHDFS(task);
                    removeFromZnode(task);
                    if (counter < MAX_OUTPUT_INFO) {
                        builder.append("[info] delete " + task
                                + separator);
                    } else if (counter == MAX_OUTPUT_INFO) {
                        builder.append("      ....." + separator);
                    }
                    counter++;
                    break;
                }
            }
            builder.append("[info] total " + counter + " items" + separator);
        } catch (KeeperException.ConnectionLossException e) {
            throw new CobwebRuntimeException.OperationFailedException("[Error] Connection loss" +
                    ", you may have to wait for a while.");
        } catch (KeeperException.AuthFailedException e) {
            throw new CobwebRuntimeException.OperationFailedException("[Error] Authentication failed.");
        } catch (KeeperException.NoAuthException e) {
            throw new CobwebRuntimeException.OperationFailedException("[Error] Permission denied.");
        } catch (Exception e) {
            throw new CobwebRuntimeException.OperationFailedException("Unknow error. " + e.getMessage());
        }
        return builder.toString();
    }

    public void removeFromZnode(String taskName) throws Exception {
        client.delete().forPath(ZNodeStaticSetting.TASKS_PATH + '/' + taskName);
    }

    public void removeFromHDFS(String taskName)
            throws IOException {
        dfsManager.delete(configuration.WAITING_TASKS_URLS + '/' + taskName, false);
    }

    public List<String> getTasksName() throws Exception {
        return client.getChildren().forPath(ZNodeStaticSetting.TASKS_PATH);
    }

    @Override
    public void sumbit() {

    }
}
