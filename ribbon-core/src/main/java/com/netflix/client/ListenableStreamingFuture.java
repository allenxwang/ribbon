package com.netflix.client;

import java.util.concurrent.Future;

public interface ListenableStreamingFuture<START extends IResponse, END extends IResponse, T> extends Future<END> {
    public void addListener(StreamingResponseCallback<START, END, T> callback);
}
