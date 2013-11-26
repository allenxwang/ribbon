package com.netflix.client.netty.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.CancellationException;

import com.netflix.client.ListenableFuture;
import com.netflix.client.ResponseCallback;

public class ExecutionPromise<T> extends DefaultPromise<T> implements ListenableFuture<T> {

    private volatile ChannelFuture channelFuture;
    
    private class RibbonChannelListener implements GenericFutureListener<Future<T>> {

        private final ResponseCallback<T> callback;
        
        RibbonChannelListener(ResponseCallback<T> callback) {
            this.callback = callback;
        }
        @Override
        public void operationComplete(Future<T> future) throws Exception {
            if (callback != null) {
                if (future.isSuccess()) {
                    callback.completed(future.get());
                } else if (future.isCancelled()) {
                    callback.failed(new CancellationException());
                } else if (!future.isSuccess()) {
                    callback.failed(future.cause());
                }
            }
        }
    }
    
    
    public ExecutionPromise() {
        super();
    }

    public ExecutionPromise(EventExecutor executor) {
        super(executor);
    }

    @Override
    public void addListener(ResponseCallback<T> callback) {
        this.addListener(new RibbonChannelListener(callback));
    }

    void setChannelFuture(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
    }
    
    @Override
    public boolean cancel(boolean mayInterrupt) {
        if (!this.isCancellable()) {
            return false;
        }
        boolean cancelled = false;
        if (channelFuture != null) {
            if (channelFuture.isCancellable()) {
                cancelled = channelFuture.cancel(mayInterrupt);
            } else if (channelFuture.isSuccess()) {
                Channel ch = channelFuture.channel();
                try {
                    cancelled = ch.disconnect().sync().isSuccess();
                } catch (InterruptedException e) {
                }
            }
        } 
        if (cancelled) {
            return super.cancel(mayInterrupt);
        }
        return cancelled;
    }

    @Override
    protected EventExecutor executor() {
        if (channelFuture != null && channelFuture.isSuccess()) {
            return channelFuture.channel().eventLoop();
        }
        return super.executor();
    }
    
    
}
