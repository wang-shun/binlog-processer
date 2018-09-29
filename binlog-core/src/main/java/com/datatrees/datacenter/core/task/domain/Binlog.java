package com.datatrees.datacenter.core.task.domain;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Binlog implements Serializable {

  Pattern p = Pattern
    .compile("^((25[0-5]|2[0-4]\\d|[1]{1}\\d{1}\\d{1}|[1-9]{1}\\d{1}|\\d{1})($|(?!\\.$)\\.)){4}$");
  Matcher m = p.matcher("254.249.199.9");
  private String jdbcUrl;
  /**
   * binlog 唯一编号
   */
  private String identity;
  /**
   * 解析的binlog的完整路径
   */
  private String path;

  public Binlog() {
  }


  public Binlog(String path, String identity, String jdbcUrl) {
    this.path = path;
    this.jdbcUrl = jdbcUrl;
    this.identity = identity;
  }

  public static void main(String[] args) {
    System.out.println("10.10.1.1".replaceAll("\\.", "#"));
  }

  public String getInstanceId() {
//    Matcher m = p.matcher(jdbcUrl);
    if (PropertiesUtility.defaultProperties().getProperty("SERVER_TYPE")
      .equalsIgnoreCase("aliyun")) {
      return jdbcUrl.substring(0, jdbcUrl.indexOf(".")).
        replaceAll("\\d+", "");
    } else {
      return jdbcUrl.replaceAll("\\.", "_");
    }
  }

  public String getIdentity() {
    return identity;
  }

  public void setIdentity(String identity) {
    this.identity = identity;
  }

  public String getIdentity0() {
    return getIdentity().split("_")[0];
  }

  public String getIdentity1() {
    return getIdentity().split("_")[1];
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("[" + this.getIdentity() + ",");
    stringBuilder.append(this.getJdbcUrl() + ",");
    stringBuilder.append(this.getPath() + "]");
    return stringBuilder.toString();
  }
}
