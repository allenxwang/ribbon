package com.netflix.client;

public interface StreamingResponseCallback<START extends IResponse, END extends IResponse, T> extends ResponseCallback<END>{
    
    /**
     * Invoked when the initial response is received. For example, the status code and headers
     * of HTTP response is received.
     */
    // public void responseReceived(T response);

    /**
     * Invoked when decoded content is delivered from {@link StreamDecoder}.
     */
    // public void contentReceived(E content);    
    
    public void started(START start);
    
    public void contentReceived(T obj);
}
