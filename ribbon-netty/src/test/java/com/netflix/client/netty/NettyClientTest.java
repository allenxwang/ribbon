package com.netflix.client.netty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import rx.util.functions.Action1;

import com.google.common.collect.Lists;
import com.netflix.client.ResponseCallback;
import com.netflix.client.StreamDecoder;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.client.http.HttpResponse;
import com.netflix.client.netty.http.AsyncNettyHttpClient;
import com.netflix.client.netty.http.NettyHttpLoadBalancerErrorHandler;
import com.netflix.client.netty.http.RxNettyHttpClient;
import com.netflix.loadbalancer.AvailabilityFilteringRule;
import com.netflix.loadbalancer.BaseLoadBalancer;
import com.netflix.loadbalancer.DummyPing;
import com.netflix.loadbalancer.RoundRobinRule;
import com.netflix.loadbalancer.Server;
import com.netflix.ribbon.test.client.ResponseCallbackWithLatch;
import com.netflix.ribbon.test.resources.EmbeddedResources;
import com.netflix.ribbon.test.resources.EmbeddedResources.Person;
import com.netflix.serialization.ContentTypeBasedSerializerKey;
import com.netflix.serialization.TypeDef;
import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.net.httpserver.HttpServer;

public class NettyClientTest {
    
    private static HttpServer server = null;
    private static String SERVICE_URI;

    private AsyncNettyHttpClient client = new AsyncNettyHttpClient(
            DefaultClientConfigImpl.getClientConfigWithDefaultValues()
            .withProperty(CommonClientConfigKey.ReadTimeout, "5000"));
    private static int port;

    public static class ResponseCallbackWithLatchForType<T> implements ResponseCallback<T> {
        private volatile T obj;
        private volatile boolean cancelled;
        private volatile Throwable error;

        private CountDownLatch latch = new CountDownLatch(1);
        private AtomicInteger totalCount = new AtomicInteger();
        
        public final T get() {
            return obj;
        }

        public final boolean isCancelled() {
            return cancelled;
        }

        public final Throwable getError() {
            return error;
        }
        
        @Override
        public void completed(T obj) {
            this.obj = obj;
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
            System.err.println("ResponseCallbackWithLatch: cancelled");
            this.cancelled = true;
            latch.countDown();    
            totalCount.incrementAndGet();
        }
        
        @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "RV_RETURN_VALUE_IGNORED")
        public void awaitCallback() throws InterruptedException {
            latch.await(60, TimeUnit.SECONDS); // NOPMD
            // wait more time in case duplicate callback is received
            Thread.sleep(1000);
            if (getFinalCount() > 1) {
                fail("Duplicate callback received");
            } else if (getFinalCount() == 0) {
                fail("No callback received");
            }
                
        }
        
        public long getFinalCount() {
            return totalCount.get();
        }
        
    }

    static class SSEDecoder implements StreamDecoder<String, ByteBuf> {

        @Override
        public String decode(ByteBuf input) throws IOException {
            if (input == null || !input.isReadable()) {
                return null;
            }
            ByteBuf buffer = Unpooled.buffer();
            int start = input.readerIndex();
            boolean foundDelimiter = false;
            while (input.readableBytes() > 0) {
                byte b = input.readByte();
                if (b == 10 || b == 13) {
                    foundDelimiter = true;
                    break;
                } else {
                    buffer.writeByte(b);
                }
            }
            if (!foundDelimiter) {
                input.readerIndex(start);
                return null;
            }
            int bytesRead = input.readerIndex() - start;
            if (bytesRead == 0) {
                return null;
            }
            byte[] content = new byte[buffer.readableBytes()];
            buffer.getBytes(0, content);
            return new String(content, "UTF-8");
        }        
    }
    
    @BeforeClass 
    public static void init() throws Exception {
        PackagesResourceConfig resourceConfig = new PackagesResourceConfig("com.netflix.ribbon.test.resources");
        port = (new Random()).nextInt(1000) + 4000;
        SERVICE_URI = "http://localhost:" + port + "/";
        ExecutorService service = Executors.newFixedThreadPool(200);
        try{
            server = HttpServerFactory.create(SERVICE_URI, resourceConfig);           
            server.setExecutor(service);
            server.start();
        } catch(Exception e) {
            e.printStackTrace();
            fail("Unable to start server");
        }
    }

    @Test
    public void testExternal() throws Exception {
        for (int i = 0; i < 1; i++) {
            URI uri = new URI("http://www.google.com:80/");
            HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
            ResponseCallbackWithLatch callback = new  ResponseCallbackWithLatch();
            client.execute(request, TypeDef.<HttpResponse>fromClass(HttpResponse.class))
                   .addListener(callback);
            callback.awaitCallback();
            if (callback.getError() != null) {
                callback.getError().printStackTrace();
            }
            assertEquals(200, callback.getHttpResponse().getStatus());
            System.err.println("Test " + i + " done");
        }
    }
    
    @Test
    public void testObservable() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        RxNettyHttpClient observableClient = new RxNettyHttpClient();
        final List<Person> result = Lists.newArrayList();
        observableClient.execute(request, TypeDef.fromClass(Person.class)).toBlockingObservable().forEach(new Action1<Person>() {
            @Override
            public void call(Person t1) {
                try {
                    result.add(t1);
                    System.err.println("added");
                } catch (Exception e) { // NOPMD
                }
            }
        });
        assertEquals(Lists.newArrayList(EmbeddedResources.defaultPerson), result);
    }
    
    @Test
    public void testGet() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatchForType<Person> callback = new  ResponseCallbackWithLatchForType<Person>();
        client.execute(request, TypeDef.fromClass(Person.class)).addListener(callback);
        callback.awaitCallback();
        assertEquals(EmbeddedResources.defaultPerson, callback.get());
    }
    /*
    @Test
    public void testStream() throws Exception {
        HttpRequest request = HttpRequest.newBuilder().uri(SERVICE_URI + "testAsync/stream").build();
        final List<String> results = Lists.newArrayList();
        final CountDownLatch latch = new CountDownLatch(1);
        client.execute(request, new SSEDecoder(), new ResponseCallback<HttpResponse, String>() {
            @Override
            public void completed(HttpResponse response) {
                latch.countDown();    
            }

            @Override
            public void failed(Throwable e) {
                e.printStackTrace();
            }

            @Override
            public void contentReceived(String element) {
                System.out.println("received: " + element);
                results.add(element);
            }

            @Override
            public void cancelled() {
            }

            @Override
            public void responseReceived(HttpResponse response) {
            }
        });
        latch.await(60, TimeUnit.SECONDS); // NOPMD
        assertEquals(EmbeddedResources.streamContent, results);
    }
    */
    
    @Test
    public void testNoEntity() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/noEntity");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        final AtomicInteger responseCode = new AtomicInteger();
        final AtomicBoolean hasEntity = new AtomicBoolean(true);
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch() {
            @Override
            public void completed(HttpResponse response) {
                super.completed(response);
                responseCode.set(response.getStatus());
                hasEntity.set(response.hasEntity());
            }            
        };
        client.execute(request, TypeDef.fromClass(HttpResponse.class)).addListener(callback);
        callback.awaitCallback();
        assertNull(callback.getError());
        assertEquals(200, responseCode.get());
        assertFalse(hasEntity.get());
    }


    @Test
    public void testPost() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/person");
        Person myPerson = new Person("netty", 5);
        HttpRequest request = HttpRequest.newBuilder().uri(uri).verb(Verb.POST).entity(myPerson).header("Content-type", "application/json").build();
        ResponseCallbackWithLatchForType<Person> callback = new ResponseCallbackWithLatchForType<Person>();        
        client.execute(request, TypeDef.fromClass(Person.class)).addListener(callback);
        callback.awaitCallback();        
        assertEquals(myPerson, callback.get());
    }

    @Test
    public void testQuery() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/personQuery");
        Person myPerson = new Person("hello world", 4);
        HttpRequest request = HttpRequest.newBuilder().uri(uri).queryParams("age", String.valueOf(myPerson.age))
                .queryParams("name", myPerson.name).build();
        
        ResponseCallbackWithLatchForType<Person> callback = new ResponseCallbackWithLatchForType<Person>();        
        client.execute(request, TypeDef.fromClass(Person.class)).addListener(callback);
        callback.awaitCallback();        
        assertEquals(myPerson, callback.get());
    }

    @Test
    public void testConnectTimeout() throws Exception {
        AsyncNettyHttpClient timeoutClient = new AsyncNettyHttpClient(DefaultClientConfigImpl.getClientConfigWithDefaultValues().withProperty(CommonClientConfigKey.ConnectTimeout, "1"));
        HttpRequest request = HttpRequest.newBuilder().uri("http://www.google.com:81/").build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        timeoutClient.execute(request, TypeDef.fromClass(HttpResponse.class)).addListener(callback);
        callback.awaitCallback();
        assertNotNull(callback.getError());
    }

    @Test
    public void testReadTimeout() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/readTimeout");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        client.execute(request, TypeDef.fromClass(HttpResponse.class)).addListener(callback);
        callback.awaitCallback();
        assertNotNull(callback.getError());
    }
    
    @Test
    public void testFuture() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        Future<HttpResponse> future = client.execute(request, TypeDef.fromClass(HttpResponse.class));
        HttpResponse response = future.get();
        // System.err.println(future.get().getEntity(Person.class));
        Person person = response.getEntity(Person.class);
        assertEquals(EmbeddedResources.defaultPerson, person);
        assertTrue(response.getHeaders().get("Content-type").contains("application/json"));
    }
    
    @Test
    public void testThrottle() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/throttle");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatchForType<Person> callback = new  ResponseCallbackWithLatchForType<Person>();
        client.execute(request, TypeDef.fromClass(Person.class)).addListener(callback);
        callback.awaitCallback();
        assertEquals("Service Unavailable", callback.getError().getMessage());
    }
    
    /*
    @Test
    public void testCancelWithLongRead() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/readTimeout");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        Future<HttpResponse> future = client.execute(request, TypeDef.fromClass(HttpResponse.class)).addListener(callback);
        // wait until channel is established
        Thread.sleep(2000);
        assertTrue(future.cancel(true));
        callback.awaitCallback();
        assertTrue(callback.isCancelled());
        try {
            future.get();
            fail("Exception expected since future is cancelled");
        } catch (Exception e) {
            assertNotNull(e);
        }
    }
    
    @Test
    public void testCancelWithConnectionIssue() throws Exception {
        HttpRequest request = HttpRequest.newBuilder().uri("http://www.google.com:81/").build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        Future<HttpResponse> future = client.execute(request, callback);
        assertTrue(future.cancel(true));
        callback.awaitCallback();
        assertTrue(callback.isCancelled());
        try {
            future.get();
            fail("Exception expected since future is cancelled");
        } catch (Exception e) {
            assertNotNull(e);
        }
    }
    
    @Test
    public void testObservable() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ObservableAsyncClient<HttpRequest, HttpResponse, ByteBuf> observableClient = 
               new ObservableAsyncClient<HttpRequest, HttpResponse, ByteBuf>(client);
        final List<Person> result = Lists.newArrayList();
        observableClient.execute(request).toBlockingObservable().forEach(new Action1<HttpResponse>() {
            @Override
            public void call(HttpResponse t1) {
                try {
                    result.add(t1.getEntity(Person.class));
                } catch (Exception e) { // NOPMD
                }
            }
        });
        assertEquals(Lists.newArrayList(EmbeddedResources.defaultPerson), result);
        assertEquals(EmbeddedResources.defaultPerson, observableClient.execute(request).toBlockingObservable().single().getEntity(Person.class));
    }
    
    @Test
    public void testStreamObservable() throws Exception {
        HttpRequest request = HttpRequest.newBuilder().uri(SERVICE_URI + "testAsync/stream").build();
        final List<String> results = Lists.newArrayList();
        ObservableAsyncClient<HttpRequest, HttpResponse, ByteBuf> observableClient = 
                new ObservableAsyncClient<HttpRequest, HttpResponse, ByteBuf>(client);
        observableClient.stream(request, new SSEDecoder())
            .toBlockingObservable()
            .forEach(new Action1<StreamEvent<HttpResponse, String>>() {

                @Override
                public void call(final StreamEvent<HttpResponse, String> t1) {
                    results.add(t1.getEvent());
                }
            });                
        assertEquals(EmbeddedResources.streamContent, results);
    }
    
    @Test
    public void testLoadBalancingClient() throws Exception {
        AsyncLoadBalancingClient<HttpRequest, HttpResponse, ByteBuf, ContentTypeBasedSerializerKey> loadBalancingClient = new AsyncLoadBalancingClient<HttpRequest, 
                HttpResponse, ByteBuf, ContentTypeBasedSerializerKey>(client);
        BaseLoadBalancer lb = new BaseLoadBalancer(new DummyPing(), new AvailabilityFilteringRule());
        List<Server> servers = Lists.newArrayList(new Server("localhost:" + port));
        lb.setServersList(servers);
        loadBalancingClient.setLoadBalancer(lb);
        URI uri = new URI("/testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        loadBalancingClient.execute(request, callback);
        callback.awaitCallback();        
        assertEquals(EmbeddedResources.defaultPerson, callback.getHttpResponse().getEntity(Person.class));
        assertEquals(1, lb.getLoadBalancerStats().getSingleServerStat(new Server("localhost:" + port)).getTotalRequestsCount());        
    }
    
    @Test
    public void testLoadBalancingClientFuture() throws Exception {
        AsyncLoadBalancingClient<HttpRequest, HttpResponse, ByteBuf, ContentTypeBasedSerializerKey> loadBalancingClient = new AsyncLoadBalancingClient<HttpRequest, 
                HttpResponse, ByteBuf, ContentTypeBasedSerializerKey>(client);
        BaseLoadBalancer lb = new BaseLoadBalancer(new DummyPing(), new AvailabilityFilteringRule());
        List<Server> servers = Lists.newArrayList(new Server("localhost:" + port));
        lb.setServersList(servers);
        loadBalancingClient.setLoadBalancer(lb);
        URI uri = new URI("/testAsync/person");
        Person person = null;
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        Future<HttpResponse> future = loadBalancingClient.execute(request);
        HttpResponse response = future.get();
        person = response.getEntity(Person.class);
        assertEquals(EmbeddedResources.defaultPerson, person);
        assertEquals(1, lb.getLoadBalancerStats().getSingleServerStat(new Server("localhost:" + port)).getTotalRequestsCount());        
    }

    
    @Test
    public void testLoadBalancingClientMultiServers() throws Exception {
        AsyncLoadBalancingClient<HttpRequest, HttpResponse, ByteBuf, ?> loadBalancingClient = new AsyncLoadBalancingClient<HttpRequest, 
                HttpResponse, ByteBuf, ContentTypeBasedSerializerKey>(client);
        BaseLoadBalancer lb = new BaseLoadBalancer(new DummyPing(), new RoundRobinRule());
        Server good = new Server("localhost:" + port);
        Server bad = new Server("localhost:" + 33333);
        List<Server> servers = Lists.newArrayList(bad, bad, good);
        lb.setServersList(servers);
        loadBalancingClient.setLoadBalancer(lb);
        loadBalancingClient.setMaxAutoRetriesNextServer(2);
        URI uri = new URI("/testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        loadBalancingClient.execute(request, callback);
        callback.awaitCallback();       
        assertEquals(EmbeddedResources.defaultPerson, callback.getHttpResponse().getEntity(Person.class));
        assertEquals(1, lb.getLoadBalancerStats().getSingleServerStat(good).getTotalRequestsCount());
    }
    
    @Test
    public void testLoadBalancingClientConcurrency() throws Exception {
        final AsyncLoadBalancingClient<HttpRequest, HttpResponse, ByteBuf, ?> loadBalancingClient = new AsyncLoadBalancingClient<HttpRequest, 
                HttpResponse, ByteBuf, ContentTypeBasedSerializerKey>(client);
        BaseLoadBalancer lb = new BaseLoadBalancer(new DummyPing(), new RoundRobinRule());
        final Server good = new Server("localhost:" + port);
        final Server bad = new Server("localhost:" + 33333);
        List<Server> servers = Lists.newArrayList(good, bad, good, good);
        lb.setServersList(servers);
        loadBalancingClient.setLoadBalancer(lb);
        loadBalancingClient.setMaxAutoRetriesNextServer(2);
        URI uri = new URI("/testAsync/person");
        final HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        int concurrency = 200;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        final CountDownLatch completeLatch = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++) {
            final int num = i;
            final ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        loadBalancingClient.execute(request, callback);
                        callback.awaitCallback();       
                        completeLatch.countDown();
                    } catch (Throwable e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                    if (callback.getError() == null) {
                        try {
                            assertEquals(EmbeddedResources.defaultPerson, callback.getHttpResponse().getEntity(Person.class));
                        } catch (Exception e) {
                            e.printStackTrace();
                            fail(e.getMessage());
                        }
                    }
                }
            });     
        }
        if (!completeLatch.await(60, TimeUnit.SECONDS)) {
            fail("Not all threads have completed");
        }
    }

    @Test
    public void testLoadBalancingClientMultiServersFuture() throws Exception {
        BaseLoadBalancer lb = new BaseLoadBalancer(new DummyPing(), new RoundRobinRule());
        Server good = new Server("localhost:" + port);
        Server bad = new Server("localhost:" + 33333);
        List<Server> servers = Lists.newArrayList(bad, bad, good);
        lb.setServersList(servers);
        AsyncLoadBalancingClient<HttpRequest, 
        HttpResponse, ByteBuf, ContentTypeBasedSerializerKey> lbClient = new AsyncLoadBalancingClient<HttpRequest, 
                HttpResponse, ByteBuf, ContentTypeBasedSerializerKey>(client); 
        lbClient.setLoadBalancer(lb);        
        lbClient.setMaxAutoRetriesNextServer(2);
        URI uri = new URI("/testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        Future<HttpResponse> future = lbClient.execute(request);
        assertEquals(EmbeddedResources.defaultPerson, future.get().getEntity(Person.class));
        assertEquals(1, lb.getLoadBalancerStats().getSingleServerStat(good).getTotalRequestsCount());
    }

    @Test
    public void testParallel() throws Exception {
        Server good = new Server("localhost:" + port);
        Server bad = new Server("localhost:" + 33333);
        List<Server> servers = Lists.newArrayList(bad, bad, good, good);
        AsyncLoadBalancingClient<HttpRequest, HttpResponse, ByteBuf, ContentTypeBasedSerializerKey> loadBalancingClient = new AsyncLoadBalancingClient<HttpRequest, 
                HttpResponse, ByteBuf, ContentTypeBasedSerializerKey>(client); 
        BaseLoadBalancer lb = new BaseLoadBalancer(new DummyPing(), new RoundRobinRule());
        lb.setServersList(servers);
        loadBalancingClient.setLoadBalancer(lb);
        URI uri = new URI("/testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();                
        ExecutionResult<HttpResponse> result = loadBalancingClient.executeWithBackupRequests(request, 4, 1, TimeUnit.MILLISECONDS,null, callback);
        callback.awaitCallback();
        assertTrue(result.isResponseReceived());
        assertNull(callback.getError());
        assertFalse(callback.isCancelled());
        assertEquals(EmbeddedResources.defaultPerson, callback.getHttpResponse().getEntity(Person.class));
        
        // System.err.println("test done");
        for (Future<HttpResponse> future: result.getAllAttempts().values()) {
            if (!future.isDone()) {
                fail("All futures should be done at this point");
            }
        }
        assertEquals(new URI("http://" + good.getHost() + ":" + good.getPort() +  uri.getPath()), result.getExecutedURI());
    }
    
    @Test
    public void testParallelAllFailed() throws Exception {
        AsyncLoadBalancingClient<HttpRequest, HttpResponse, ByteBuf, ContentTypeBasedSerializerKey> loadBalancingClient = new AsyncLoadBalancingClient<HttpRequest, 
                HttpResponse, ByteBuf, ContentTypeBasedSerializerKey>(client);
        BaseLoadBalancer lb = new BaseLoadBalancer(new DummyPing(), new RoundRobinRule());
        Server bad = new Server("localhost:" + 55555);
        Server bad1 = new Server("localhost:" + 33333);
        List<Server> servers = Lists.newArrayList(bad, bad1, bad);
        lb.setServersList(servers);
        loadBalancingClient.setLoadBalancer(lb);
        URI uri = new URI("/testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();                
        ExecutionResult<HttpResponse> result = loadBalancingClient.executeWithBackupRequests(request, 2, 1, TimeUnit.MILLISECONDS, null, callback);
        // make sure we do not get more than 1 callback
        callback.awaitCallback();
        assertNotNull(callback.getError());
        assertTrue(result.isFailed());
    }
    
    @Test
    public void testLoadBalancingClientWithRetry() throws Exception {
        
        AsyncNettyHttpClient timeoutClient = 
                new AsyncNettyHttpClient(DefaultClientConfigImpl.getClientConfigWithDefaultValues().withProperty(CommonClientConfigKey.ConnectTimeout, "1"));
        AsyncLoadBalancingClient<HttpRequest, HttpResponse, ByteBuf, ContentTypeBasedSerializerKey> loadBalancingClient = new AsyncLoadBalancingClient<HttpRequest, 
                HttpResponse, ByteBuf, ContentTypeBasedSerializerKey>(timeoutClient);
        loadBalancingClient.setErrorHandler(new NettyHttpLoadBalancerErrorHandler());
        loadBalancingClient.setMaxAutoRetries(1);
        loadBalancingClient.setMaxAutoRetriesNextServer(1);
        Server server = new Server("www.microsoft.com:81");
        List<Server> servers = Lists.newArrayList(server);
        BaseLoadBalancer lb = new BaseLoadBalancer(new DummyPing(), new AvailabilityFilteringRule());
        lb.setServersList(servers);
        loadBalancingClient.setLoadBalancer(lb);
        URI uri = new URI("/");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        loadBalancingClient.execute(request, callback);
        callback.awaitCallback();        
        assertNotNull(callback.getError());
        assertEquals(4, lb.getLoadBalancerStats().getSingleServerStat(server).getTotalRequestsCount());                
        assertEquals(4, lb.getLoadBalancerStats().getSingleServerStat(server).getSuccessiveConnectionFailureCount());                
    }
    
   
    @Test
    public void testLoadBalancingClientWithRetryFuture() throws Exception {
        AsyncNettyHttpClient timeoutClient = 
                new AsyncNettyHttpClient(DefaultClientConfigImpl.getClientConfigWithDefaultValues().withProperty(CommonClientConfigKey.ConnectTimeout, "1"));
        AsyncLoadBalancingClient<HttpRequest, HttpResponse, ByteBuf, ContentTypeBasedSerializerKey> loadBalancingClient = new AsyncLoadBalancingClient<HttpRequest, 
                HttpResponse, ByteBuf, ContentTypeBasedSerializerKey>(timeoutClient);
        loadBalancingClient.setErrorHandler(new NettyHttpLoadBalancerErrorHandler());
        loadBalancingClient.setMaxAutoRetries(1);
        loadBalancingClient.setMaxAutoRetriesNextServer(1);
        Server server = new Server("www.microsoft.com:81");
        List<Server> servers = Lists.newArrayList(server);
        BaseLoadBalancer lb = new BaseLoadBalancer(new DummyPing(), new AvailabilityFilteringRule());
        lb.setServersList(servers);
        loadBalancingClient.setLoadBalancer(lb);
        URI uri = new URI("/");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        Future<HttpResponse> future = loadBalancingClient.execute(request);
        try {
            future.get();
            fail("ExecutionException expected");
        } catch (Exception e) {
            assertTrue(e instanceof ExecutionException);
        }
            
        assertEquals(4, lb.getLoadBalancerStats().getSingleServerStat(server).getTotalRequestsCount());                
        assertEquals(4, lb.getLoadBalancerStats().getSingleServerStat(server).getSuccessiveConnectionFailureCount());                
    }
    
    @Test
    public void testConcurrentStreaming() throws Exception {
        final HttpRequest request = HttpRequest.newBuilder().uri(SERVICE_URI + "testAsync/stream").build();
        int concurrency = 200;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        final CountDownLatch completeLatch = new CountDownLatch(concurrency);
        final AtomicInteger successCount = new AtomicInteger();
        for (int i = 0; i < concurrency; i++) {
            final int num = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    final List<String> results = Lists.newArrayList();
                    final CountDownLatch latch = new CountDownLatch(1);
                    final AtomicBoolean failed = new AtomicBoolean();
                    try {
                        client.execute(request, new SSEDecoder(), new ResponseCallback<HttpResponse, String>() {
                            @Override
                            public void completed(HttpResponse response) {
                                latch.countDown();    
                            }

                            @Override
                            public void failed(Throwable e) {
                                System.err.println("Error received " + e);
                                failed.set(true);
                                latch.countDown();
                            }

                            @Override
                            public void contentReceived(String element) {
                                results.add(element);
                            }

                            @Override
                            public void cancelled() {
                            }

                            @Override
                            public void responseReceived(HttpResponse response) {
                            }
                        });
                        if (!latch.await(60, TimeUnit.SECONDS)) {
                            fail("No callback happens within timeout");
                        }
                        if (!failed.get()) {
                            successCount.incrementAndGet();
                            assertEquals(EmbeddedResources.streamContent, results);
                        } else {
                            System.err.println("Thread " + num + " failed");
                        }
                        completeLatch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Thread " + num + " failed due to unexpected exception");
                    }
                    
                }
            });
        }
        if (!completeLatch.await(60, TimeUnit.SECONDS)) {
            fail("Some threads have not completed streaming within timeout");
        }
        System.out.println("Successful streaming " + successCount.get());
    }
    */

}
