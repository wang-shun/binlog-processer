package com.datatrees.datacenter.schema.api;

public class SchemaRequest {

    private String table;

    private String database;

    private long tableNum;

    private long timeSec;

    public SchemaRequest(){

    }

    public SchemaRequest(String table, String database, long tableNum, long timeSec) {
        this.table = table;
        this.database = database;
        this.tableNum = tableNum;
        this.timeSec = timeSec;
    }

    public long getTableNum() {
        return tableNum;
    }

    public void setTableNum(long tableNum) {
        this.tableNum = tableNum;
    }

    public long getTimeSec() {
        return timeSec;
    }

    public void setTimeSec(long timeSec) {
        this.timeSec = timeSec;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
}
