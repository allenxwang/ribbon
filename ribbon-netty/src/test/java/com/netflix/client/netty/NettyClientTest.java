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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.ribbon.test.client.ResponseCallbackWithLatch;
import com.netflix.ribbon.test.resources.EmbeddedResources;
import com.netflix.ribbon.test.resources.EmbeddedResources.Person;

import com.netflix.client.BufferedResponseCallback;
import com.netflix.client.ResponseCallback;
import com.netflix.client.StreamDecoder;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.http.AsyncBufferingHttpClient;
import com.netflix.client.http.AsyncHttpClient;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpResponse;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.client.netty.http.AsyncNettyHttpClient;
import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.net.httpserver.HttpServer;

public class NettyClientTest {
    
    private static HttpServer server = null;
    private static String SERVICE_URI;

    private AsyncNettyHttpClient client = new AsyncNettyHttpClient(DefaultClientConfigImpl.getClientConfigWithDefaultValues());
    private static int port;

    static class SSEDecoder implements StreamDecoder<String, ByteBuf> {

        @Override
        public String decode(ByteBuf input) throws IOException {
            if (input == null || !input.isReadable()) {
                return null;
            }
            ByteBuf copy = input.duplicate();
            ByteBuf buffer = Unpooled.buffer();
            int start = copy.readerIndex();
            boolean foundDelimiter = false;
            while (copy.readableBytes() > 0) {
                byte b = copy.readByte();
                if (b == 10 || b == 13) {
                    foundDelimiter = true;
                    break;
                } else {
                    buffer.writeByte(b);
                }
            }
            if (!foundDelimiter) {
                return null;
            }
            int bytesRead = copy.readerIndex() - start;
            if (bytesRead == 0) {
                return null;
            }
            input.skipBytes(bytesRead);
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
        try{
            server = HttpServerFactory.create(SERVICE_URI, resourceConfig);           
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testGet() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatch callback = new  ResponseCallbackWithLatch();
        client.execute(request, callback);
        callback.awaitCallback();
        assertEquals(EmbeddedResources.defaultPerson, callback.getHttpResponse().getEntity(Person.class));
    }
    
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
        client.execute(request, callback);
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
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        client.execute(request, callback);
        callback.awaitCallback();        
        assertEquals(myPerson, callback.getHttpResponse().getEntity(Person.class));
    }

    @Test
    public void testQuery() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/personQuery");
        Person myPerson = new Person("hello world", 4);
        HttpRequest request = HttpRequest.newBuilder().uri(uri).queryParams("age", String.valueOf(myPerson.age))
                .queryParams("name", myPerson.name).build();
        
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        client.execute(request, callback);
        callback.awaitCallback();        
        assertEquals(myPerson, callback.getHttpResponse().getEntity(Person.class));
    }

    @Test
    public void testConnectTimeout() throws Exception {
        AsyncNettyHttpClient timeoutClient = new AsyncNettyHttpClient(DefaultClientConfigImpl.getClientConfigWithDefaultValues().withProperty(CommonClientConfigKey.ConnectTimeout, "1"));
        HttpRequest request = HttpRequest.newBuilder().uri("http://www.google.com:81/").build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        timeoutClient.execute(request, callback);
        callback.awaitCallback();
        assertNotNull(callback.getError());
    }

    @Test
    public void testReadTimeout() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/readTimeout");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        client.execute(request, callback);
        callback.awaitCallback();
        assertNotNull(callback.getError());
    }
    
    @Test
    public void testFuture() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/person");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        Future<HttpResponse> future = client.execute(request);
        HttpResponse response = future.get();
        // System.err.println(future.get().getEntity(Person.class));
        Person person = response.getEntity(Person.class);
        assertEquals(EmbeddedResources.defaultPerson, person);
        assertTrue(response.getHeaders().get("Content-type").contains("application/json"));
    }
    
    @Test
    public void testCancel() throws Exception {
        URI uri = new URI(SERVICE_URI + "testAsync/readTimeout");
        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        ResponseCallbackWithLatch callback = new ResponseCallbackWithLatch();        
        Future<HttpResponse> future = client.execute(request, callback);
        assertTrue(future.cancel(true));
        callback.awaitCallback();
        assertTrue(callback.isCancelled());
    }
    
    
}
