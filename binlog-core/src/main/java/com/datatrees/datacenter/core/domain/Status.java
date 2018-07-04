package com.datatrees.datacenter.core.domain;

public enum Status {

  START(0,"开始下载"),
  SUCCESS(1, "成功下载并解析文件"),
  OPENFAILED(2, "打开binlog文件错误"),
  OPERAVROWRITERFAILED(3,"打开avro文件错误"),
  SERIALIZEEVENTFAILED(4,"序列化event错误"),
  SCHEMAFAILED(5,"获取schema错误"),
  RESOVLERECORDFAILED(6,"解析binlog错误"),
  WRITERECORDFAILED(7,"写入avro文件错误"),
  COMMITRECORDFAILED(8,"提交avro文件错误"),

  OBSLETE(-9,"废弃"),
  OTHER(99,"其他错误");
  private int value;

  private String desc;

  Status(Integer status, String desc) {
    this.value = status;
    this.desc = desc;

  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }
}
