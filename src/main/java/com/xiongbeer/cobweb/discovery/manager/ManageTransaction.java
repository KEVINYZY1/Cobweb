package com.xiongbeer.cobweb.discovery.manager;

import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.xiongbeer.cobweb.conf.Configuration;
import com.xiongbeer.cobweb.conf.StaticField;
import com.xiongbeer.cobweb.conf.ZNodeStaticSetting;
import com.xiongbeer.cobweb.discovery.task.Epoch;
import com.xiongbeer.cobweb.discovery.task.TaskManager;
import com.xiongbeer.cobweb.discovery.worker.WorkersWatcher;
import com.xiongbeer.cobweb.exception.CobwebRuntimeException;
import com.xiongbeer.cobweb.filter.Filter;
import com.xiongbeer.cobweb.saver.dfs.DFSManager;
import com.xiongbeer.cobweb.utils.MD5Maker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by shaoxiong on 17-11-12.
 */
public class ManageTransaction {

    private static final Logger logger = LoggerFactory.getLogger(ManageTransaction.class);

    private DFSManager dfsManager;

    private TaskManager taskManager;

    private WorkersWatcher workersWatcher;

    private Configuration configuration = Configuration.INSTANCE;


    public ManageTransaction(DFSManager dfsManager, TaskManager taskManager, WorkersWatcher workersWatcher) {
        this.dfsManager = dfsManager;
        this.taskManager = taskManager;
        this.workersWatcher = workersWatcher;
    }

    /**
     * 用于防止用户误将znode下task删除导致任务永久失效的情况
     * <p>
     * 保证hdfs中waitingtasks与znode中wvTasks一致
     * 但是无法保证znode中wvTasks与hdfs中waitingtasks中一致
     */
    private void syncWaitingTasks() throws IOException {
        dfsManager.listFiles((String) configuration.get("waiting_tasks_urls"), false)
                .stream()
                .map(Files::getNameWithoutExtension)
                .forEach(fileName -> taskManager.asyncSubmit(fileName));
    }

    /**
     * 检查已经完成的任务，把对应的
     * 存放url的文件移出等待队列
     */
    private Map<String, Epoch> checkTasks() throws InterruptedException, IOException {
        Map<String, Epoch> unfinishedTaskMap = new HashMap<>();

        /* 更新tasksInfo状态表 */
        taskManager.checkTasks();
        Map<String, Epoch> tasks = taskManager.getTasksInfo();
        for (Map.Entry<String, Epoch> entry : tasks.entrySet()) {
            String key = entry.getKey();
            Epoch value = entry.getValue();
            switch (value.getStatus()) {
                case FINISHED:
                    if (unfinishedTaskMap.containsKey(key)) {
                        unfinishedTaskMap.remove(key);
                    }
                    dfsManager.move(configuration.get("waiting_tasks_urls") + "/" + key,
                            configuration.get("finished_tasks_urls") + "/" + key);
                    taskManager.asyncReleaseTask(ZNodeStaticSetting.TASKS_PATH + "/" + key);
                    break;
                case RUNNING:
                    unfinishedTaskMap.put(key, value);
                    break;
                case WAITING:
                    unfinishedTaskMap.remove(key);
                    break;
                default:
                    break;
            }
        }
        return unfinishedTaskMap;
    }

    /**
     * 检查Workers的状态，若它失效则需要重置它之前领取的任务。
     */
    private void checkWorkers(Map<String, Epoch> unfinishedTaskMap) throws InterruptedException {
        workersWatcher.refreshAliveWorkers();
        workersWatcher.refreshAllWorkersStatus();
        Map<String, String> workersMap = workersWatcher.getWorkersMap();
        unfinishedTaskMap.entrySet()
                .stream()
                .filter(entry -> {
                    String name = entry.getKey();
                    Epoch epoch = entry.getValue();
                    return !workersMap.containsKey(name)
                            && epoch.getDifference() > (int) configuration.get("worker_dead_time");
                })
                .forEach(entry -> {
                    String name = entry.getKey();
                    taskManager.asyncResetTask(ZNodeStaticSetting.TASKS_PATH + "/" + name);
                    logger.warn("The owner of task: " + name + " has dead, now reset it...");
                });
    }

    /**
     * 发布新的任务
     */
    private void publishNewTasks(Filter filter) throws IOException, CobwebRuntimeException.FilterOverflowException {
        String tempSavePath = (String) configuration.get("bloom_temp_dir");
        List<String> hdfsUrlFiles = dfsManager.listFiles(
                (String) configuration.get("new_tasks_urls"), false);
        if (hdfsUrlFiles.size() == 0) {
            /* 没有需要处理的新URL文件 */
            return;
        }


        /*
            TODO
            目前是先将所有要处理的文件下载下来再进行处理，为什么不开多个线程进行处理呢，这样不是能先下载的文件先进行处理，不被IO阻塞吗？
            原因是需要对文件进行指定大小的切片，多线程的情况下切片比较难处理，还需要一些预处理，未来会将这里改为多线程
         */
        List<String> urlFiles = downloadTaskFiles(hdfsUrlFiles, tempSavePath);

        /*
            后续整体工作流程：
            对每个文件进行逐个按行读取，在开始读的同时也会
            在本地新建一个同名的.bak文件，每读一行后会尝试将
            其录入过滤器，若成功则说明此url是新的url，会将
            其写入.bak文件中。完毕后会删除其他非.bak的文件
            ，再去掉.bak文件的.bak后缀。
            然后将处理后的所有文件上传到hdfs上，确保上传成
            功后才会到znode中发布任务
        */
        filterUrlAndSave(filter, urlFiles, tempSavePath);
        File file = new File(tempSavePath);
        deleteNormalFiles(file);
        removeTempSuffix(file);
        submitNewTasks(file);

        /*
            TODO：
            当文件很大时会占用大量IO
            需要另外一种方式来备份
            目前想参照fsimage-edits的模式
        */
        /* 备份 */
        backUpFilterCache(filter);

        /*
            在hdfs上删除处理
            完毕的的new url文件
         */
        for (String urlPath : hdfsUrlFiles) {
            dfsManager.delete(urlPath, false);
        }
    }

    /**
     * 删除多余文件(不以Configuration.TEMP_SUFFIX结尾的文件)
     *
     * @param tempSaveDir
     */
    private void deleteNormalFiles(File tempSaveDir) {
        Optional.ofNullable(tempSaveDir.listFiles()).ifPresent(files ->
                Arrays.stream(files)
                        .filter(File::isFile)
                        .filter(file -> !file.getAbsolutePath().endsWith(StaticField.TEMP_SUFFIX))
                        .forEach(File::delete)
        );
    }

    /**
     * 去除文件的Configuration.TEMP_SUFFIX后缀
     *
     * @param dir
     */
    private void removeTempSuffix(File dir) {
        Optional.ofNullable(dir.listFiles()).ifPresent(files ->
                Arrays.stream(files)
                        .filter(File::isFile)
                        .filter(file -> file.getAbsolutePath().endsWith(StaticField.TEMP_SUFFIX))
                        .forEach(file -> {
                            String path = file.getAbsolutePath();
                            file.renameTo(new File(path.substring(0,
                                    path.length() - StaticField.TEMP_SUFFIX.length())));
                        })
        );
    }

    /**
     * 上传新任务到HDFS，然后发布任务到ZooKeeper中
     *
     * @param dir
     * @throws IOException
     */
    private void submitNewTasks(File dir) throws IOException {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (!file.isDirectory()) {
                String filePath = file.getAbsolutePath();
                /*
                    若文件已存在则直接跳过
                    可以这么做的原因是文件名是根据内容生成的md5码
                    相同则基本可以确定就是同一个文件，没必要重复上传
                */
                if (!dfsManager.exist(filePath)) {
                    dfsManager.uploadFile(filePath,
                            configuration.get("waiting_tasks_urls") + "/" + file.getName());
                }
                taskManager.asyncSubmit(file.getName());
            }
        }
    }

    /**
     * 从hdfs下新任务文件下载到本地
     *
     * @param urlFiles hdfs中的文件路径
     * @param savePath 保存路径
     * @return
     * @throws IOException
     */
    private List<String> downloadTaskFiles(List<String> urlFiles
            , String savePath) throws IOException {
        List<String> localUrlFiles = new LinkedList<>();
        for (String filePath : urlFiles) {
            /*
                若文件已存在则直接跳过
                可以这么做的原因是文件名是根据内容生成的md5码
                相同则基本可以确定就是同一个文件，没必要重复下载
            */
            File temp = new File(filePath);
            File localFile = new File(savePath
                    + File.separator + temp.getName());
            if (!localFile.exists()) {
                dfsManager.downloadFile(filePath, savePath);
            }
            localUrlFiles.add(localFile.getAbsolutePath());
        }
        return localUrlFiles;
    }

    /**
     * 备份filter的缓存文件到hdfs
     * 注意：目前而言，会删除原来的旧缓存文件（无论是本地还是hdfs中）
     *
     * @throws IOException
     */
    public void backUpFilterCache(Filter filter) throws IOException {
        /* 备份之前删除原来的缓存文件 */
        File localSave = new File((String) configuration.get("bloom_save_path"));
        Optional.ofNullable(localSave.listFiles())
                .ifPresent(files ->
                        Arrays.stream(files)
                                .filter(File::isFile)
                                .forEach(File::delete));
        /* 备份至本地 */
        String bloomFilePath = filter.save((String) configuration.get("bloom_save_path"));
        /* 上传至dfs */
        dfsManager.uploadFile(bloomFilePath, (String) configuration.get("bloom_backup_path"));

        /* 删除dfs上旧的缓存文件，去除新缓存文件的TEMP_SUFFIX后缀 */
        List<String> cacheFiles
                = dfsManager.listFiles((String) configuration.get("bloom_backup_path"), false);
        for (String cache : cacheFiles) {
            if (!cache.endsWith(StaticField.TEMP_SUFFIX)) {
                dfsManager.delete(cache, false);
            } else {
                String newName = cache.substring(0,
                        cache.length() - StaticField.TEMP_SUFFIX.length());
                dfsManager.move(cache, newName);
            }
        }

    }

    /**
     * 遍历下载下来的保存着url的文件
     * 以行为单位将其放入过滤器
     * 过滤后的url会被以固定的数量
     * 切分为若干个文件
     *
     * @param urlFiles
     * @throws IOException
     */
    private void filterUrlAndSave(Filter filter, List<String> urlFiles, final String saveDir) throws IOException {
        /* 用AtomicLong的原因只是为了能在匿名类中计数 */
        final AtomicLong newUrlCounter = new AtomicLong(0);
        final StringBuilder newUrls = new StringBuilder();
        final MD5Maker md5 = new MD5Maker();
        for (String filePath : urlFiles) {
            File file = new File(filePath);
            /* 每读一定数量的URLS就将其写入新的文件 */
            Files.readLines(file,
                    Charset.defaultCharset(), new LineProcessor<Object>() {
                        @Override
                        public boolean processLine(String line) throws IOException {
                            String newLine = line + System.getProperty("line.separator");
                            /* 到filter中确认url是不是已经存在，已经存在就丢弃 */
                            if (filter.put(line)) {
                                md5.update(newLine);
                                if (newUrlCounter.get() <= (int) configuration.get("task_urls_num")) {
                                    newUrls.append(newLine);
                                    newUrlCounter.incrementAndGet();
                                } else {
                                    /* 文件名是根据其内容生成的md5值 */
                                    String urlFileName = saveDir + File.separator
                                            + md5.toString()
                                            + StaticField.TEMP_SUFFIX;
                                    Files.write(newUrls.toString().getBytes()
                                            , new File(urlFileName));
                                    newUrls.delete(0, newUrls.length());
                                    newUrlCounter.set(0);
                                    md5.reset();
                                }
                            }
                            return true;
                        }

                        @Override
                        public Object getResult() {
                            /* 处理残留的urls */
                            if (newUrls.length() > 0) {
                                String urlFileName = saveDir + File.separator
                                        + md5.toString()
                                        + StaticField.TEMP_SUFFIX;
                                try {
                                    Files.write(newUrls.toString().getBytes()
                                            , new File(urlFileName));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                newUrls.delete(0, newUrls.length());
                                newUrlCounter.set(0);
                            }
                            return null;
                        }
                    });
        }
    }
}
