package com.netflix.client.netty.http;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.netflix.client.AsyncClient;
import com.netflix.client.BufferedResponseCallback;
import com.netflix.client.ClientException;
import com.netflix.client.IClientConfigAware;
import com.netflix.client.ResponseCallback;
import com.netflix.client.StreamDecoder;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.client.config.IClientConfigKey;
import com.netflix.client.http.AsyncHttpClient;
import com.netflix.serialization.ContentTypeBasedSerializerKey;
import com.netflix.serialization.JacksonSerializationFactory;
import com.netflix.serialization.SerializationFactory;
import com.netflix.serialization.Serializer;

public class AsyncNettyHttpClient 
implements AsyncClient<com.netflix.client.http.HttpRequest, com.netflix.client.http.HttpResponse, ByteBuf, ContentTypeBasedSerializerKey>, 
AsyncHttpClient<ByteBuf>,
IClientConfigAware {

    private SerializationFactory<ContentTypeBasedSerializerKey> serializationFactory = new JacksonSerializationFactory();
    private Bootstrap b = new Bootstrap();

    private static final String RIBBON_HANDLER = "ribbonHandler"; 
    private final String READ_TIMEOUT_HANDLER = "readTimeoutHandler"; 

    private ExecutorService executors;

    private int readTimeout;
    private int connectTimeout;
    private boolean executeCallbackInSeparateThread = true;

    private static final Logger logger = LoggerFactory.getLogger(AsyncNettyHttpClient.class);

    public static final IClientConfigKey InvokeNettyCallbackInSeparateThread = new IClientConfigKey() {
        @Override
        public String key() {
            return "InvokeNettyCallbackInSeparateThread";
        }
    };

    public AsyncNettyHttpClient() {
        this(DefaultClientConfigImpl.getClientConfigWithDefaultValues(), new NioEventLoopGroup());
    }

    public AsyncNettyHttpClient(IClientConfig config) {
        this(config, new NioEventLoopGroup());
    }

    public AsyncNettyHttpClient(IClientConfig config, EventLoopGroup group) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(group);
        b.group(group)
        .channel(NioSocketChannel.class)
        .handler(new Initializer());
        initWithNiwsConfig(config);
    }

    @Override
    public void initWithNiwsConfig(IClientConfig config) {
        String serializationFactoryClass = config.getPropertyAsString(CommonClientConfigKey.DefaultSerializationFactoryClassName, 
                JacksonSerializationFactory.class.getName());
        if (serializationFactoryClass != null) {
            try {
                serializationFactory = (SerializationFactory<ContentTypeBasedSerializerKey>) Class.forName(serializationFactoryClass).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Unable to initialize SerializationFactory", e);
            }
        }
        executeCallbackInSeparateThread = config.getPropertyAsBoolean(InvokeNettyCallbackInSeparateThread, false);
        connectTimeout = config.getPropertyAsInteger(CommonClientConfigKey.ConnectTimeout, DefaultClientConfigImpl.DEFAULT_CONNECT_TIMEOUT);
        readTimeout = config.getPropertyAsInteger(CommonClientConfigKey.ReadTimeout, DefaultClientConfigImpl.DEFAULT_READ_TIMEOUT);
    }

    private static class Initializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();

            p.addLast("log", new LoggingHandler(LogLevel.INFO));
            p.addLast("codec", new HttpClientCodec());

            // Remove the following line if you don't want automatic content decompression.
            p.addLast("inflater", new HttpContentDecompressor());
        }        
    }

    private class RibbonHttpChannelInboundHandler<E> extends SimpleChannelInboundHandler<HttpObject> {
        HttpResponse response;
        AtomicBoolean channelRead = new AtomicBoolean(false);
        AtomicBoolean readTimeoutOccured = new AtomicBoolean(false);

        private ResponseCallback<com.netflix.client.http.HttpResponse, E> callback;
        private StreamDecoder<E, ByteBuf> decoder;
        private URI uri;
        private FutureHelper future;
        
        RibbonHttpChannelInboundHandler(ResponseCallback<com.netflix.client.http.HttpResponse, E> callback, final StreamDecoder<E, ByteBuf> streamDecoder, URI uri, FutureHelper future) {
            this.callback = callback;
            this.uri = uri;
            this.decoder = streamDecoder;
            this.future = future;
        }
        
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg)
                throws Exception {
            synchronized (this) {
                if (!readTimeoutOccured.get()) {
                    channelRead.set(true);
                } else {
                    return;
                }
            }
            if (msg instanceof HttpResponse) {
                HttpResponse response = (HttpResponse) msg;
                this.response = response;
                NettyHttpResponse nettyResponse = new NettyHttpResponse(this.response, null, serializationFactory, uri);
                if (callback != null) {
                    callback.responseReceived(nettyResponse);
                }
            } 
            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                final ByteBuf buf = content.content();
                buf.retain();
                if (decoder != null) {
                    ctx.fireChannelRead(buf);
                }
                if (content instanceof LastHttpContent) {
                    NettyHttpResponse nettyResponse = new NettyHttpResponse(this.response, buf, serializationFactory, uri);
                    future.lock.lock();
                    try {
                        future.completeResponse.set(nettyResponse);
                        future.waitingForCompletion.signalAll();
                        System.err.println("Got full response, signalAll");
                    } finally {
                        future.lock.unlock();
                    }
                    ctx.close();
                    if (callback != null) {
                        invokeResponseCallback(callback, nettyResponse);
                    }
                }
            }
            
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            synchronized (this) {
                if (cause instanceof io.netty.handler.timeout.ReadTimeoutException) {
                    if (channelRead.get()) {
                        // channel read already happened, ignore this read timeout
                        return;
                    } else {
                        readTimeoutOccured.set(true);
                    }
                }
            }
            future.lock.lock();
            try {
                future.error.set(cause);
                future.waitingForCompletion.signalAll();
            } finally {
                future.lock.unlock();
            }
            if (callback != null) {
                invokeExceptionCallback(callback, cause);
            }
            ctx.channel().close();
            ctx.close();
        }
    }

    private static class FutureHelper implements Future<com.netflix.client.http.HttpResponse> {

        ChannelFuture channelFuture;
        ReentrantLock lock = new ReentrantLock();
        Condition waitingForCompletion = lock.newCondition();
        AtomicReference<com.netflix.client.http.HttpResponse> completeResponse = new AtomicReference<com.netflix.client.http.HttpResponse>();
        AtomicReference<Throwable> error = new AtomicReference<Throwable>();
        AtomicBoolean cancelled = new AtomicBoolean(false);
        ResponseCallback<com.netflix.client.http.HttpResponse, ?> callback;
        
        FutureHelper(ChannelFuture channelFuture, ResponseCallback<com.netflix.client.http.HttpResponse, ?> callback) {
            this.channelFuture = channelFuture;
            this.callback = callback;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (!channelFuture.isDone()) {
                return channelFuture.cancel(mayInterruptIfRunning);
            } else if (channelFuture.isSuccess()) {
                if (mayInterruptIfRunning) {
                    lock.lock();
                    try {
                        Channel ch = channelFuture.channel();
                        ch.pipeline().remove(RIBBON_HANDLER);
                        ch.disconnect();
                        cancelled.set(true);
                        waitingForCompletion.signalAll();
                        if (callback != null) {
                            callback.cancelled();
                        }
                        return true;
                    } finally {
                        lock.unlock();
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        @Override
        public com.netflix.client.http.HttpResponse get()
                throws InterruptedException, ExecutionException {
            lock.lock();
            try {
                while (completeResponse.get() == null && error.get() == null && !cancelled.get()) {
                    waitingForCompletion.await();
                }
                if (completeResponse.get() != null) {
                    return completeResponse.get();
                } else if (error.get() != null) {
                    throw new ExecutionException(error.get());
                } else {
                    // cancelled
                    throw new ExecutionException(new ClientException("operation cancelled"));
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public com.netflix.client.http.HttpResponse get(long arg0, TimeUnit arg1)
                throws InterruptedException, ExecutionException,
                TimeoutException {
            lock.lock();
            try {
                if (completeResponse.get() == null || error.get() == null || !cancelled.get()) {
                    boolean completed = waitingForCompletion.await(arg0, arg1);
                    if (!completed) {
                        throw new TimeoutException("Timed out");
                    }
                }
                if (completeResponse.get() != null) {
                    return completeResponse.get();
                } else if (error.get() != null) {
                    throw new ExecutionException(error.get());
                } else {
                    // cancelled
                    throw new ExecutionException(new ClientException("operation cancelled"));
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean isCancelled() {
            if (channelFuture.isCancelled()) {
                return true;
            }
            lock.lock();
            try {
                return cancelled.get();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean isDone() {
            if (isCancelled()) {
                return true;
            } else {
                lock.lock();
                try {
                    return completeResponse.get() != null || error.get() != null || cancelled.get();
                } finally {
                    lock.unlock();
                }
            }
        }
        
    }
    
    @Override
    public <E> Future<com.netflix.client.http.HttpResponse> execute(
            final com.netflix.client.http.HttpRequest request,
            final StreamDecoder<E, ByteBuf> decoder,
            final ResponseCallback<com.netflix.client.http.HttpResponse, E> callback)
                    throws ClientException {
        final URI uri = request.getUri();
        String scheme = uri.getScheme() == null? "http" : uri.getScheme();
        String host = uri.getHost();
        int port = uri.getPort();
        if (port == -1) {
            if ("http".equalsIgnoreCase(scheme)) {
                port = 80;
            } else if ("https".equalsIgnoreCase(scheme)) {
                port = 443;
            }
        }
        final HttpRequest nettyHttpRequest = getHttpRequest(request);
        // Channel ch = null;
        final FutureHelper future;
        try {
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
            ChannelFuture channelFuture = b.connect(host, port);
            future = new FutureHelper(channelFuture, callback);
            channelFuture.addListener(new ChannelFutureListener() {         
                @Override
                public void operationComplete(final ChannelFuture f) {
                    future.lock.lock();
                    try {
                        if (f.isCancelled()) {
                            future.cancelled.set(true);
                            future.waitingForCompletion.signalAll();
                            callback.cancelled();
                        } else if (!f.isSuccess()) {
                            future.error.set(f.cause());
                            future.waitingForCompletion.signalAll();
                            callback.failed(f.cause());
                        } else {
                            final Channel ch = f.channel();
                            final ChannelPipeline p = ch.pipeline();

                            if (decoder == null) {
                                p.addLast("aggregator", new HttpObjectAggregator(Integer.MAX_VALUE));
                            }
                            // only add read timeout after successful channel connection
                            if (p.get(READ_TIMEOUT_HANDLER) != null) {
                                p.remove(READ_TIMEOUT_HANDLER);
                            }
                            p.addLast(READ_TIMEOUT_HANDLER, new ReadTimeoutHandler(readTimeout, TimeUnit.MILLISECONDS));

                            if (p.get(RIBBON_HANDLER) != null) {
                                p.remove(RIBBON_HANDLER);
                            }

                            p.addLast(RIBBON_HANDLER, new RibbonHttpChannelInboundHandler<E>(callback, decoder, uri, future));
                            if (decoder != null) {
                                p.addLast("EntityDecoder", new ByteToMessageDecoder() {
                                    @Override
                                    protected void decode(
                                            ChannelHandlerContext ctx,
                                            ByteBuf in, List<Object> out)
                                                    throws Exception {
                                        Object obj = decoder.decode(in);
                                        if (obj != null) {
                                            out.add(obj);
                                        }
                                        // in.release();                                    
                                    }
                                });
                                p.addLast("EntityHandler", new SimpleChannelInboundHandler<E>() {
                                    @Override
                                    protected void channelRead0(
                                            ChannelHandlerContext ctx, E msg)
                                                    throws Exception {
                                        if (callback != null && msg != null) {
                                            callback.contentReceived(msg);
                                        }
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                        if (callback != null) {
                                            callback.failed(cause);
                                        }
                                    }

                                });
                            }
                            ch.writeAndFlush(nettyHttpRequest);
                        };
                    } catch (Throwable e) {
                        // this will be called if task submission is rejected
                        future.error.set(e);
                        future.waitingForCompletion.signalAll();
                        if (callback != null) {
                            callback.failed(e);
                        }
                    } finally {
                        future.lock.unlock();
                    }
                }
            });
            return future;
        } catch (Exception e) {
            throw new ClientException(e);
        }
    }

    private void invokeResponseCallback(final ResponseCallback<com.netflix.client.http.HttpResponse, ?> callback, final NettyHttpResponse nettyResponse) {
        callback.completed(nettyResponse);
    }

    private void invokeExceptionCallback(final ResponseCallback<com.netflix.client.http.HttpResponse, ?> callback, final Throwable e) {
        callback.failed(e);
    }

    private static String getContentType(Map<String, Collection<String>> headers) {
        if (headers == null) {
            return null;
        }
        for (Map.Entry<String, Collection<String>> entry: headers.entrySet()) {
            String key = entry.getKey();
            if (key.equalsIgnoreCase("content-type")) {
                Collection<String> values = entry.getValue();
                if (values != null && values.size() > 0) {
                    return values.iterator().next();
                }
            }
        }
        return null;
    }

    private HttpRequest getHttpRequest(com.netflix.client.http.HttpRequest request) throws ClientException {
        HttpRequest r = null;
        Object entity = request.getEntity();
        String uri = request.getUri().getRawPath();
        if (request.getQueryParams() != null) {
            QueryStringEncoder encoder = new QueryStringEncoder(uri);
            for (Map.Entry<String, Collection<String>> entry: request.getQueryParams().entrySet()) {
                String name = entry.getKey();
                Collection<String> values = entry.getValue();
                for (String value: values) {
                    encoder.addParam(name, value);
                }
            }
            uri = encoder.toString();
        }
        if (entity != null) {
            String contentType = getContentType(request.getHeaders());    
            ContentTypeBasedSerializerKey key = new ContentTypeBasedSerializerKey(contentType, entity.getClass());
            Serializer serializer = serializationFactory.getSerializer(key).orNull();
            if (serializer == null) {
                throw new ClientException("Unable to find serializer for " + key);
            }
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            try {
                serializer.serialize(bout, entity);
            } catch (IOException e) {
                throw new ClientException("Error serializing entity in request", e);
            }
            byte[] content = bout.toByteArray();
            ByteBuf buf = Unpooled.wrappedBuffer(content);
            r = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(request.getVerb().name()), uri, buf);
            r.headers().set(HttpHeaders.Names.CONTENT_LENGTH, content.length);
        } else {
            r = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(request.getVerb().name()), uri);
        }
        if (request.getHeaders() != null) {
            for (Map.Entry<String, Collection<String>> entry: request.getHeaders().entrySet()) {
                String name = entry.getKey();
                Collection<String> values = entry.getValue();
                r.headers().set(name, values);
            }
        }

        return r;
    }

    public void shutDown() {
        executors.shutdown();
    }

    public final SerializationFactory<ContentTypeBasedSerializerKey> getSerializationFactory() {
        return serializationFactory;
    }

    public final void setSerializationFactory(
            SerializationFactory<ContentTypeBasedSerializerKey> serializationFactory) {
        this.serializationFactory = serializationFactory;
    }

    public final int getReadTimeout() {
        return readTimeout;
    }

    public final void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public final int getConnectTimeout() {
        return connectTimeout;
    }

    public final void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public final boolean isExecuteCallbackInSeparateThread() {
        return executeCallbackInSeparateThread;
    }

    public final void setExecuteCallbackInSeparateThread(
            boolean executeCallbackInSeparateThread) {
        this.executeCallbackInSeparateThread = executeCallbackInSeparateThread;
    }

    @Override
    public Future<com.netflix.client.http.HttpResponse> execute(
            com.netflix.client.http.HttpRequest request,
            BufferedResponseCallback<com.netflix.client.http.HttpResponse> callback)
                    throws ClientException {
        return execute(request, null, callback);
    }

    @Override
    public void addSerializationFactory(
            SerializationFactory<ContentTypeBasedSerializerKey> factory) {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public Future<com.netflix.client.http.HttpResponse> execute(
            com.netflix.client.http.HttpRequest request) throws ClientException {
        return execute(request, null, null);
    }

}
