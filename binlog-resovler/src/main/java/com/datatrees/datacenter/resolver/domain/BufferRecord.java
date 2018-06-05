//package com.datatrees.datacenter.resolver.domain;
//
//import org.apache.avro.Schema;
//
//import java.io.Serializable;
//import java.util.AbstractMap;
//
//public class BufferRecord {
//    /**
//     * binlog file unique name
//     */
//    String identity;
//    String instanceId;
//
//    AbstractMap.SimpleEntry<Schema, Object> resultValue;
//
//    AbstractMap.SimpleEntry<String, String> tableSchema;
//    Serializable[] beforeValue;
//    Serializable[] afterValue;
//    Operator operator;
//
//    public AbstractMap.SimpleEntry<Schema, Object> getResultValue() {
//        return resultValue;
//    }
//
//    public void setResultValue(AbstractMap.SimpleEntry<Schema, Object> resultValue) {
//        this.resultValue = resultValue;
//    }
//
//    public BufferRecord() {
//
//    }
//
//    public BufferRecord(String identity, String instanceId, Operator operator, Serializable[] beforeValue, Serializable[] afterValue) {
//        this.instanceId = instanceId;
//        this.identity = identity;
//        this.operator = operator;
//        this.beforeValue = beforeValue;
//        this.afterValue = afterValue;
//    }
//
//    public void setTableSchema(AbstractMap.SimpleEntry<String, String> tableSchema) {
//        this.tableSchema = tableSchema;
//    }
//
//    public Serializable[] getBeforeValue() {
//        return beforeValue;
//    }
//
//    public void setBeforeValue(Serializable[] beforeValue) {
//        this.beforeValue = beforeValue;
//    }
//
//    public Serializable[] getAfterValue() {
//        return afterValue;
//    }
//
//    public void setAfterValue(Serializable[] afterValue) {
//        this.afterValue = afterValue;
//    }
//
//    public Operator getOperator() {
//        return operator;
//    }
//
//    public void setOperator(Operator operator) {
//        this.operator = operator;
//    }
//
//    public String getIdentity() {
//        return identity;
//    }
//
//    public void setIdentity(String identity) {
//        this.identity = identity;
//    }
//
//}
