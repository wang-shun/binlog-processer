package com.datatrees.datacenter.resolver.reader;

public interface EventConsumer<T, Result> {
    void consume(T record);

    void onSuccess(Result result);
}
