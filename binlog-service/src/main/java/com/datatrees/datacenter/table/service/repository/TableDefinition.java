package com.datatrees.datacenter.table.service.repository;

public class TableDefinition {

    private String schema;

    private String table;

    public TableDefinition(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    @Override
    public boolean equals(Object object){

        if (! (object instanceof TableDefinition)){
            return false;
        }

        TableDefinition target = (TableDefinition) object;
        return target.schema.equals(schema) && target.table.equals(table);
    }

    @Override
    public int hashCode(){
        return (schema + table).hashCode();
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
