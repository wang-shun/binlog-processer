package com.datatrees.datacenter.resolver.domain;

import com.github.shyiko.mysql.binlog.event.EventType;

public enum Operator {
    C() {
        @Override
        public String toString() {
            return "Create";
        }
    },
    U() {
        @Override
        public String toString() {
            return "Update";
        }
    },
    D() {
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
                return Operator.C;
            case "Update":
                return Operator.U;
            case "Delete":
                return Operator.D;
            default:
                return Operator.DEFAULT;
        }
    }

    public static Operator valueOf(EventType eventType) {
        switch (eventType) {
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                return Operator.C;
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                return Operator.U;
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                return Operator.D;
            default:
                return Operator.DEFAULT;
        }
    }
}
