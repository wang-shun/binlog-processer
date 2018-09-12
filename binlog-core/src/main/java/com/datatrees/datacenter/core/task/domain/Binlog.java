package com.datatrees.datacenter.core.task.domain;

import java.io.Serializable;

public class Binlog implements Serializable {

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

  public String getInstanceId() {
    if (jdbcUrl.split("\\.").length == 1) {
      return jdbcUrl;
    } else {
      return jdbcUrl.substring(0, jdbcUrl.indexOf(".")).
        replaceAll("\\d+", "");
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
    stringBuilder.append("[" + this.getJdbcUrl() + ",");
    stringBuilder.append(this.getPath() + "]");
    return stringBuilder.toString();
  }
}
