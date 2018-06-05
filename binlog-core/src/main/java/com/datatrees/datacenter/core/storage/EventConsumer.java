package com.datatrees.datacenter.core.storage;

@FunctionalInterface
public interface EventConsumer<T> {
    void consume(T bufferRecord);
}