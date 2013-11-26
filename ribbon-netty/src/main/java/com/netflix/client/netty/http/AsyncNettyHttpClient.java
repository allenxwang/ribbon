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
import io.netty.handler.codec.http.FullHttpResponse;
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
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import com.netflix.client.ClientException;
import com.netflix.client.IClientConfigAware;
import com.netflix.client.ListenableFuture;
import com.netflix.client.ResponseCallback;
import com.netflix.client.StreamDecoder;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.client.config.IClientConfigKey;
import com.netflix.client.http.AsyncHttpClient;
import com.netflix.client.http.HttpResponseCallback;
import com.netflix.serialization.ContentTypeBasedSerializerKey;
import com.netflix.serialization.JacksonSerializationFactory;
import com.netflix.serialization.SerializationFactory;
import com.netflix.serialization.Serializer;
import com.netflix.serialization.TypeDef;

public class AsyncNettyHttpClient implements AsyncHttpClient, IClientConfigAware {

    private SerializationFactory<ContentTypeBasedSerializerKey> serializationFactory = new JacksonSerializationFactory();
    private Bootstrap b = new Bootstrap();

    public static final String RIBBON_HANDLER = "ribbonHandler"; 
    public static final String READ_TIMEOUT_HANDLER = "readTimeoutHandler";
    public static final String AGGREGATOR = "aggegator"; 
    public static final String ENTITY_DECODER = "EntityDecoder";
    public static final String ENTITY_HANDLER = "EntityHandler";

    private int readTimeout;
    private int connectTimeout;
    private boolean executeCallbackInSeparateThread = true;
    private EventLoopGroup eventGroup;

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
        eventGroup = group;
        b.group(group)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_KEEPALIVE, true)
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
        connectTimeout = config.getPropertyAsInteger(CommonClientConfigKey.ConnectTimeout, DefaultClientConfigImpl.DEFAULT_CONNECT_TIMEOUT);
        readTimeout = config.getPropertyAsInteger(CommonClientConfigKey.ReadTimeout, DefaultClientConfigImpl.DEFAULT_READ_TIMEOUT);
    }

    private class Initializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();

            // p.addLast("log", new LoggingHandler(LogLevel.INFO));
            p.addLast("codec", new HttpClientCodec());

            p.addLast("inflater", new HttpContentDecompressor());
            
            p.addLast(AGGREGATOR, new HttpObjectAggregator(Integer.MAX_VALUE));
            
            p.addLast(READ_TIMEOUT_HANDLER, new ReadTimeoutHandler(readTimeout, TimeUnit.MILLISECONDS));

        }        
    }

    private class RibbonHttpChannelInboundHandler<T> extends SimpleChannelInboundHandler<HttpObject> {
        AtomicBoolean channelRead = new AtomicBoolean(false);
        AtomicBoolean readTimeoutOccured = new AtomicBoolean(false);

        private URI uri;
        private TypeDef<T> typeDef;
        ExecutionPromise<T> promise;

        RibbonHttpChannelInboundHandler(URI uri, TypeDef<T> typeDef, ExecutionPromise<T> promise) {
            this.uri = uri;
            this.typeDef = typeDef;
            this.promise = promise;
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
            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                response.content().retain();
                if (this.typeDef.getRawType().isAssignableFrom(com.netflix.client.http.HttpResponse.class)) {
                    NettyHttpResponse nettyResponse = new NettyHttpResponse(response, response.content(), serializationFactory, uri);
                    promise.trySuccess((T) nettyResponse);
                } else if (!this.typeDef.getRawType().isAssignableFrom(Void.class)) {
                    ctx.fireChannelRead(response);
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
            promise.tryFailure(cause);
        }
    }

    @Override
    public <T> ListenableFuture<T> execute(final com.netflix.client.http.HttpRequest request, 
            final TypeDef<T> typeDef) {
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
        // final AttributeKey<ExecutionPromise<T>> promiseAttrKey = new AttributeKey<ExecutionPromise<T>>(PROCESSING_PROMISE);
        final HttpRequest nettyHttpRequest;
        final ExecutionPromise<T> promise = new ExecutionPromise<T>(this.eventGroup.next());
        try {
            nettyHttpRequest = getHttpRequest(request);
        } catch (Throwable e) {
            promise.setFailure(e);
            return promise;
        }
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
        ChannelFuture channelFuture = b.connect(host, port);
        promise.setChannelFuture(channelFuture);
        channelFuture.addListener(new ChannelFutureListener() {         
            @Override
            public void operationComplete(final ChannelFuture f) {
                try {
                    if (f.isCancelled()) {
                        promise.setFailure(new CancellationException());
                    } else if (!f.isSuccess()) {
                        promise.setFailure(f.cause());
                    } else {
                        final Channel ch = f.channel();
                        // ch.attr(promiseAttrKey).set(promise);
                        final ChannelPipeline p = ch.pipeline();
                        // only add read timeout after successful channel connection
                        if (p.get(RIBBON_HANDLER) != null) {
                            p.remove(RIBBON_HANDLER);
                        }

                        p.addLast(RIBBON_HANDLER, new RibbonHttpChannelInboundHandler<T>(uri, typeDef, promise));
                        
                        p.addLast("Decoder", new HttpEntityDecoder<T>(serializationFactory, typeDef));
                        // p.addLast(handlers)

                        p.addLast("Final", new SimpleChannelInboundHandler<T>() {
                            @Override
                            protected void channelRead0(
                                    ChannelHandlerContext ctx, T msg)
                                            throws Exception {
                                promise.trySuccess(msg);
                            }

                            @Override
                            public void exceptionCaught(
                                    ChannelHandlerContext ctx, Throwable cause)
                                    throws Exception {
                                promise.tryFailure(cause);
                            }
                        });

                        ChannelFuture future = ch.writeAndFlush(nettyHttpRequest);
                        promise.setChannelFuture(future);
                        future.addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (f.isCancelled()) {
                                    promise.cancel(true);
                                } else if (!f.isSuccess()) {
                                    promise.tryFailure(f.cause());
                                }
                            }
                        });
                    };
                } catch (Throwable e) {
                    promise.tryFailure(e);
                } 
            }
        });
        return promise;
        
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
            Serializer serializer = serializationFactory.getSerializer(key);
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
            r = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(request.getVerb().name()), uri);
        }
        if (request.getHeaders() != null) {
            for (Map.Entry<String, Collection<String>> entry: request.getHeaders().entrySet()) {
                String name = entry.getKey();
                Collection<String> values = entry.getValue();
                r.headers().set(name, values);
            }
        }
        if (request.getUri().getHost() != null) {
            r.headers().set(HttpHeaders.Names.HOST, request.getUri().getHost());
        }
        return r;
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
    public void close() throws IOException {
    }
}
