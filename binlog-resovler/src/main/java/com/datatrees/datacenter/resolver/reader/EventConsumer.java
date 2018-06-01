package com.datatrees.datacenter.resolver.reader;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

@SuppressWarnings
public interface EventConsumer<T, Result> {
    void consume(T record);

    void onSuccess(Result result);
}
