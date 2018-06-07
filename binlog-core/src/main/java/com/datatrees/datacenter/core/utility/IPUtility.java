package com.datatrees.datacenter.core.utility;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IPUtility {
    public static String ipAddress() {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            return String.format("%s-%s", addr.getHostAddress().toString(), addr.getHostName().toString());
        } catch (UnknownHostException e) {
            return "Unknown";
        }
    }
}
