package cn.edu.nju.pasa.graph.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by bsidb on 12/5/2017.
 */
public class HostInfoUtil {
    public static String getHostName() {
        String hostName = null;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return hostName;
    }
}
