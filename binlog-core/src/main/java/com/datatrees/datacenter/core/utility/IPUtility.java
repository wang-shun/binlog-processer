package com.datatrees.datacenter.core.utility;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IPUtility {

  static String SERVER_ID;

  static {
    SERVER_ID = PropertiesUtility.defaultProperties().getProperty("SERVER.ID");
  }

  public static String ipAddress() {
    try {
      InetAddress addr = InetAddress.getLocalHost();
      return String
        .format("%s-%s-%s", addr.getHostAddress().toString(), addr.getHostName().toString(),
          SERVER_ID);
    } catch (UnknownHostException e) {
      return "Unknown";
    }
  }
}
