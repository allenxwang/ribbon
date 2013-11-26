package com.netflix.client;

import java.io.Closeable;
import java.util.concurrent.Future;

import com.google.common.reflect.TypeToken;

public interface AsyncStreamingClient<T extends ClientRequest, START extends IResponse, END extends IResponse, E> extends Closeable {
    /**
     * Asynchronously execute a request.
     * 
     * @param request Request to execute
     * @param decooder Decoder to decode objects from the native stream 
     * @param callback Callback to be invoked when execution completes or fails
     * @return Future of the response
     * @param <E> Type of object to be decoded from the stream
     * 
     * @throws ClientException if exception happens before the actual asynchronous execution happens, for example, an error to serialize 
     *         the entity
     */
    public ListenableStreamingFuture<START, END, E> execute(T request, StreamingResponseCallback<START, END, E> callback, TypeToken<E> type) throws ClientException;
}

