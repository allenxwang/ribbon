package com.netflix.client;

import java.util.concurrent.Future;

public interface ListenableFuture<T> extends Future<T> {
    public void addListener(ResponseCallback<T> callback);
}
