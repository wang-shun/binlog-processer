package com.datatrees.datacenter.resolver.domain;

public enum Status {
    SUCCESS(1), FAIL(0);

    Status(Integer status) {
        this.value = status;
    }

    private int value;

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
