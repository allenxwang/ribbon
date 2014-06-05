package com.netflix.loadbalancer;

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

import com.google.common.collect.Lists;
import com.netflix.client.RetryHandler;

public class LoadBalancerExecutorTest {
    
    static Server server1 = new Server("1", 80);
    static Server server2 = new Server("2", 80);
    static Server server3 = new Server("3", 80);
    
    static List<Server> list = Lists.newArrayList(server1, server2, server3);
    
    class MyLoadBalancerProvider implements ClientObservableProvider<String> {

        private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        private AtomicInteger count = new AtomicInteger();

        @Override
        public Observable<String> getObservableForEndpoint(final Server server) {
            return Observable.create(new OnSubscribe<String>() {
                @Override
                public void call(final Subscriber<? super String> t1) {
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            if (count.incrementAndGet() < 3) {
                                t1.onError(new IllegalArgumentException());
                            } else {
                                t1.onNext(server.getHost());
                                t1.onCompleted();
                            }                    
                        }                        
                    });
                        
                    
                }                
            });
        }
        
    }
    
    @Test
    public void testRetrySameServer() {
        LoadBalancerExecutor lbExecutor = LoadBalancerBuilder.newBuilder().buildFixedServerListLoadBalancerExecutor(list);
        ClientObservableProvider<String> observableProvider = new MyLoadBalancerProvider();
        RetryHandler handler = new RetryHandler() {
            @Override
            public boolean isRetriableException(Throwable e, boolean sameServer) {
                return (e instanceof IllegalArgumentException);
            }
            @Override
            public boolean isCircuitTrippingException(Throwable e) {
                return false;
            }
            @Override
            public int getMaxRetriesOnSameServer() {
                return 3;
            }
            @Override
            public int getMaxRetriesOnNextServer() {
                return 0;
            }
        };
        String result = lbExecutor.retrySameServer(server1, observableProvider, handler).toBlockingObservable().single();
        assertEquals(3, lbExecutor.getServerStats(server1).getTotalRequestsCount());
        assertEquals("1", result);
    }
    
    @Test
    public void testRetryNextServer() {
        LoadBalancerExecutor lbExecutor = LoadBalancerBuilder.newBuilder().buildFixedServerListLoadBalancerExecutor(list);
        ClientObservableProvider<String> observableProvider = new MyLoadBalancerProvider();
        RetryHandler handler = new RetryHandler() {
            @Override
            public boolean isRetriableException(Throwable e, boolean sameServer) {
                return (e instanceof IllegalArgumentException);
            }
            @Override
            public boolean isCircuitTrippingException(Throwable e) {
                return false;
            }
            @Override
            public int getMaxRetriesOnSameServer() {
                return 1;
            }
            @Override
            public int getMaxRetriesOnNextServer() {
                return 5;
            }
        };
        String result = lbExecutor.executeWithLoadBalancer(observableProvider, handler).toBlockingObservable().single();
        assertEquals("3", result); // server2 is picked first
        assertEquals(2, lbExecutor.getServerStats(server2).getTotalRequestsCount());
        assertEquals(1, lbExecutor.getServerStats(server3).getTotalRequestsCount());
    }


}
