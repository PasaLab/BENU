package cn.edu.nju.pasa.graph.conf;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * This configuration reader will load configurations
 * from the file `pasa.conf.prop` in the working directory.
 *
 * It uses the static class to make sure that the configuration
 * is loaded only once for the current JVM.
 * Created by zk Wang on 12/3/2017.
 */
public class ProcessLevelConfReader extends Properties {
    public static final String CONF_FILE_NAME = "pasa.conf.prop";
    private static Properties properties = null;
    static {
        try {
            FileInputStream inputStream = new FileInputStream(CONF_FILE_NAME);
            properties = new Properties();
            properties.load(inputStream);
            inputStream.close();
            System.err.println("Load configuration done!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static Properties getPasaConf() {
        return properties;
    }
}
