package com.netflix.ribbon.test.client;

import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.client.BufferedResponseCallback;
import com.netflix.client.http.HttpResponse;

public class ResponseCallbackWithLatch extends BufferedResponseCallback<HttpResponse> {
    private volatile HttpResponse httpResponse;
    private volatile boolean cancelled;
    private volatile Throwable error;

    private CountDownLatch latch = new CountDownLatch(1);
    private AtomicInteger totalCount = new AtomicInteger();
    
    public final HttpResponse getHttpResponse() {
        return httpResponse;
    }

    public final boolean isCancelled() {
        return cancelled;
    }

    public final Throwable getError() {
        return error;
    }
    
    @Override
    public void completed(HttpResponse response) {
        this.httpResponse = response;
        latch.countDown();    
        totalCount.incrementAndGet();
    }

    @Override
    public void failed(Throwable e) {
        this.error = e;
        latch.countDown();    
        totalCount.incrementAndGet();
    }

    @Override
    public void cancelled() {
        this.cancelled = true;
        latch.countDown();    
        totalCount.incrementAndGet();
    }
    
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "RV_RETURN_VALUE_IGNORED")
    public void awaitCallback() throws InterruptedException {
        latch.await(60, TimeUnit.SECONDS); // NOPMD
        // wait more time in case duplicate callback is received
        Thread.sleep(1000);
        if (getFinalCount() != 1) {
            fail("Duplicate callback received");
        }
            
    }
    
    public long getFinalCount() {
        return totalCount.get();
    }
    
}
