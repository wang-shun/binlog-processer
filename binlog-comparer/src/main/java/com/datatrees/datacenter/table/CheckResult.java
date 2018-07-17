package com.datatrees.datacenter.table;

public class CheckResult {
    private String oldId;
    private String fileName;
    private String dbInstance;
    private String dataBase;
    private String tableName;
    private String filePartition;
    private String opType;
    private String lastUpdateTime;

    public CheckResult() {
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getDbInstance() {
        return dbInstance;
    }

    public void setDbInstance(String dbInstance) {
        this.dbInstance = dbInstance;
    }

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFilePartition() {
        return filePartition;
    }

    public void setFilePartition(String filePartition) {
        this.filePartition = filePartition;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public String getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(String lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getOldId() {

        return oldId;
    }

    public void setOldId(String oldId) {
        this.oldId = oldId;
    }
}
