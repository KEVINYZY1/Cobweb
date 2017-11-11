package com.xiongbeer.cobweb.conf;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.xiongbeer.cobweb.utils.InitLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by shaoxiong on 17-11-4.
 */
public enum Configuration {
    INSTANCE;

    private final Path CONF_PATH = Paths.get("conf", "core.yml");

    private volatile Map<String, Setting> properties = new HashMap<>();

    private Logger logger = LoggerFactory.getLogger(Configuration.class);

    Configuration() {
        InitLogger.init();
        Optional<String> defaultEnvPath = Optional.of(System.getenv("COBWEB_HOME"));
        logger.info("load cobweb environment path : " + defaultEnvPath.get());
        logger.info("loading default setting...");
        try {
            init(defaultEnvPath.get());
            logger.info("loading user setting...");
            loadConf(Paths.get(defaultEnvPath.get(), CONF_PATH.toString()).toString());
            loadHDFSConf();
        } catch (IOException e) {
            logger.error("load configuration failed. ", e);
            System.exit(1);
        }
        logger.info("load configuration success.");
        System.out.println(properties);
    }

    public Object get(String setting) {
        return properties.get(setting).getResource();
    }

    private void loadConf(String cobwebRootPath) throws FileNotFoundException, YamlException {
        YamlReader reader = new YamlReader(new FileReader(cobwebRootPath));
        while (true) {
            Map contact = (Map) reader.read();
            if (contact == null) {
                break;
            }
            String name = (String) contact.get("name");
            String textValue = (String) contact.get("value");
            Object value = getValue(name, textValue);
            Setting property = new Setting(value, name);
            properties.put(name, property);
        }
    }

    public void init(String cobwebRootPath) throws IOException {
        defaultLoad("hdfs_system_path", "default");

        defaultLoad("bloom_local_save_path", cobwebRootPath + "/data/bloom");
        defaultLoad("hdfs_root", "/cobweb");

        String root = (String) properties.get("hdfs_root").getResource();
        defaultLoad("waiting_tasks_urls", root + "/tasks/waitingtasks");
        defaultLoad("finished_tasks_urls", root + "/tasks/finishedtasks");
        defaultLoad("new_tasks_urls", root + "/tasks/newurls");

        String cobwebConfPath = cobwebRootPath + File.separator + "conf";
        defaultLoad("zk_connect_string"
                , getZKConnectString(cobwebConfPath + File.separator + StaticField.ZK_SERVER_FILE_NAME));

        /* bloom过滤器会定时备份，此为其存放的路径 */
        defaultLoad("bloom_backup_path", root + "/bloom");

        /* 临时文件（UrlFile）的存放的本地路径 */
        defaultLoad("temp_dir", cobwebRootPath + "/data/temp");

        /* Worker与ZooKeeper断开连接后，经过DEADTIME后认为Worker死亡 */
        defaultLoad("worker_dead_time", 120);

        /* Manager进行检查的间隔 */
        defaultLoad("check_time", 45);

        /* 本机ip Worker节点需要配置 */
        defaultLoad("local_host", "127.0.0.1");

        /* Worker服务使用的端口 Worker节点需要配置 */
        defaultLoad("local_port", 22000);

        /* 命令行API服务所使用的端口 */
        defaultLoad("local_shell_port", 22001);

        /* bloom过滤器过滤url文件的暂存位置 */
        defaultLoad("bloom_temp_dir", cobwebRootPath + "/data/bloom/temp");

        /* 均衡负载server端默认端口 */
        defaultLoad("balance_server_port", 8081);

        /* 每个任务包含的URL的最大数量 */
        defaultLoad("task_urls_num", 200);

        /* zookeeper的session过期时间 */
        defaultLoad("zk_session_timeout", 40000);

        /* zookeeper客户端初始化连接等待的最长时间 */
        defaultLoad("zk_init_timeout", 10000);

        /* zookeeper客户端断开后的重试次数 */
        defaultLoad("zk_retry_times", 3);

        /* zookeeper客户端重试时的时间间隔 */
        defaultLoad("zk_retry_interval", 2000);

        /* HDFS文件系统的nameservice路径 */
        defaultLoad("hdfs_system_path", "");

        /* worker接取任务后的心跳频率 */
        defaultLoad("worker_heart_beat", 15);

        /* tomcat服务器刷新数据的间隔 */
        defaultLoad("tomcat_heart_beat", 5);

        /* 异步执行的线程数量 */
        defaultLoad("local_async_thread_num", Runtime.getRuntime().availableProcessors());
    }

    private void defaultLoad(String settingName, Object resource) {
        properties.put(settingName, new Setting(resource, settingName));
    }

    private Object getValue(String textName, String textValue) {
        switch (textName) {
            case "hdfs_system_path":
                if (textValue.equals("default")) {
                    Optional<String> defaultHadoopPath = Optional.of(System.getenv("HADOOP_HOME"));
                    logger.info("load cobweb environment path : " + defaultHadoopPath.get());
                    return defaultHadoopPath.get();
                }
            case "check_time":
            case "task_urls_num":
            case "zk_session_timeout":
            case "tomcat_heart_beat":
            case "worker_heart_beat":
            case "local_port":
            case "local_shell_port":
            case "balance_server_port":
                return Integer.parseInt(textValue);
            default:
                return textValue;
        }
    }

    private String getZKConnectString(String filePath) throws IOException {
        List<String> content = Files.readLines(new File(filePath), Charset.defaultCharset());
        return Joiner.on(',').skipNulls().join(content) + ZNodeStaticSetting.ROOT_PATH;
    }

    private void loadHDFSConf() {
        String hadoopConfPath = (String) get("hdfs_system_path");
        org.apache.hadoop.conf.Configuration res = new org.apache.hadoop.conf.Configuration();
        res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "core-site.xml").toString());
        res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "hdfs-site.xml").toString());
        res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "mapred-site.xml").toString());
        res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "yarn-site.xml").toString());
        defaultLoad("hdfs_system_conf", res);
    }

    private static class Setting {
        private final Object resource;

        private final String name;

        public Setting(Object resource, String name) {
            this.resource = resource;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public Object getResource() {
            return resource;
        }

        @Override
        public String toString() {
            return "[" + name + " : " + resource + "]";
        }
    }
}
