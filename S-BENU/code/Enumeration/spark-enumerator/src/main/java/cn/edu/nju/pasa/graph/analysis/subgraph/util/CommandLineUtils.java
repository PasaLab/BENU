package cn.edu.nju.pasa.graph.analysis.subgraph.util;

import org.apache.commons.cli.*;

import java.util.Properties;

/**
 * Created by Zhaokang Wang on 12/4/2017.
 */
public class CommandLineUtils {

    public static Properties parseArgs(String[] args) throws Exception {
        Properties prop = new Properties();
        for (int i = 0; i < args.length; i ++) {
            if (!args[i].contains("=")) {
                throw new Exception("argument " + args[i] + " misses a value.");
            }
            String option = args[i].split("=")[0];
            String value = args[i].split("=")[1];
            prop.setProperty(option, value);
        }
        return prop;
    }
}
