package com.datatrees.datacenter.resolver.domain;

import com.github.shyiko.mysql.binlog.event.EventType;

public enum Operator {
    Create() {
        @Override
        public String toString() {
            return "Create";
        }
    },
    Update() {
        @Override
        public String toString() {
            return "Update";
        }
    },
    Delete() {
        @Override
        public String toString() {
            return "Delete";
        }
    },
    DEFAULT() {
        @Override
        public String toString() {
            return "nothing";
        }
    };

    public static Operator parse(String operator) {
        switch (operator) {
            case "Create":
                return Operator.Create;
            case "Update":
                return Operator.Update;
            case "Delete":
                return Operator.Delete;
            default:
                return Operator.DEFAULT;
        }
    }

    public static Operator valueOf(EventType eventType) {
        switch (eventType) {
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                return Operator.Create;
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                return Operator.Update;
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                return Operator.Delete;
            default:
                return Operator.DEFAULT;
        }
    }
}
