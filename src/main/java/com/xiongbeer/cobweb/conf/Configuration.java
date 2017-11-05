package com.xiongbeer.cobweb.conf;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
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
        Optional<String> hadoopEnvPath = Optional.of(System.getenv("HADOOP_HOME"));
        logger.info("load hadoop environment path : ", hadoopEnvPath.get());
        Optional<String> defaultEnvPath = Optional.of(System.getenv("COBWEB_HOME"));
        logger.info("load cobweb environment path : ", defaultEnvPath.get());

        try {
            loadConf(Paths.get(defaultEnvPath.get(), CONF_PATH.toString()).toString(), hadoopEnvPath.get());
        } catch (FileNotFoundException | YamlException e) {
            logger.error("load configuration failed. ", e.getMessage());
            System.exit(1);
        }
    }

    private void loadConf(String cobwebConfPath, String hadoopConfPath) throws FileNotFoundException, YamlException {
        YamlReader reader = new YamlReader(new FileReader(cobwebConfPath));
        logger.info("loading static text setting...");
        while (true) {
            Map contact = (Map) reader.read();
            if (contact == null) {
                break;
            }
            String name = (String) contact.get("name");
            String textValue = (String) contact.get("value");
            Object value = getValue(textValue, hadoopConfPath);
            Setting property = new Setting(value, name);
            properties.put(name, property);
        }
    }

    private Object getValue(String textValue, String hadoopConfPath) {
        switch (textValue) {
            case "hdfs_system_conf":
                org.apache.hadoop.conf.Configuration res = new org.apache.hadoop.conf.Configuration();
                // TODO 非默认情况
                if (textValue.equals("default")) {
                    res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "core-site.xml").toString());
                    res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "hdfs-site.xml").toString());
                    res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "mapred-site.xml").toString());
                    res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "yarn-site.xml").toString());
                }
                return res;
            default:
                return textValue;
        }
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
            return name;
        }
    }
}
