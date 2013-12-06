package com.netflix.client.netty.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.netflix.client.ClientException;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpResponse;
import com.netflix.serialization.ContentTypeBasedSerializerKey;
import com.netflix.serialization.JacksonSerializationFactory;
import com.netflix.serialization.SerializationFactory;
import com.netflix.serialization.Serializer;
import com.netflix.serialization.TypeDef;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;
import rx.Observable;
import rx.Observer;
import rx.netty.protocol.http.ChannelSetting;
import rx.netty.protocol.http.HttpMessageObserver;
import rx.netty.protocol.http.HttpProtocolHandlerAdapter;
import rx.netty.protocol.http.ObservableHttpClient;
import rx.netty.protocol.http.ObservableHttpResponse;
import rx.netty.protocol.http.ValidatedFullHttpRequest;
import rx.subjects.PublishSubject;
import rx.util.functions.Func1;

public class RxNettyHttpClient {

    private ObservableHttpClient observableClient;
    private SerializationFactory<ContentTypeBasedSerializerKey> serializationFactory;
    private int connectTimeout;
    private int readTimeout;
    
    public RxNettyHttpClient() {
        this(DefaultClientConfigImpl.getClientConfigWithDefaultValues());        
    }
    
    public RxNettyHttpClient(IClientConfig config) {
        this.connectTimeout = config.getPropertyAsInteger(CommonClientConfigKey.ConnectTimeout, DefaultClientConfigImpl.DEFAULT_CONNECT_TIMEOUT);
        this.readTimeout = config.getPropertyAsInteger(CommonClientConfigKey.ReadTimeout, DefaultClientConfigImpl.DEFAULT_READ_TIMEOUT);  
        this.serializationFactory = new JacksonSerializationFactory();
        this.observableClient = ObservableHttpClient.newBuilder()
                .withChannelOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
                .build(new NioEventLoopGroup());
        
    }
    
    private class SingleEntityHandler<T> extends HttpProtocolHandlerAdapter<T> {
        private HttpEntityDecoder<T> decoder;
        
        private SingleEntityHandler(HttpRequest request, SerializationFactory<ContentTypeBasedSerializerKey> serializationFactory, TypeDef<T> typeDef) {
            decoder = new HttpEntityDecoder<T>(serializationFactory, request, typeDef);
        }

        @Override
        public void configure(ChannelPipeline pipeline,
                Observer<? super ObservableHttpResponse<T>> observer,
                Observer<T> entityObserver) {
            super.configure(pipeline, observer, entityObserver);
            pipeline.addAfter("http-aggregator", "entity-decoder", decoder);
            if (pipeline.get("content-handler") != null) {
                pipeline.remove("content-handler");
            }
            // pipeline.addLast("readtimeout-handler", new ReadTimeoutHandler(readTimeout, TimeUnit.MILLISECONDS));
            pipeline.addLast("entity-handler", new EntityHandler<T>(observer, entityObserver));
        }
    }
    
    
    private class FullHttpResponseHandler extends HttpProtocolHandlerAdapter<FullHttpResponse> {
        @Override
        public void configure(ChannelPipeline pipeline,
                Observer<? super ObservableHttpResponse<FullHttpResponse>> observer,
                Observer<FullHttpResponse> entityObserver) {
            super.configure(pipeline, observer, entityObserver);
            if (pipeline.get("content-handler") != null) {
                pipeline.remove("content-handler");
            }
            // pipeline.addLast("readtimeout-handler", new ReadTimeoutHandler(readTimeout, TimeUnit.MILLISECONDS));
            pipeline.addLast("entity-handler", new EntityHandler<FullHttpResponse>(observer, entityObserver));
        }
    }

    
    private static class EntityHandler<T> extends ChannelInboundHandlerAdapter {
        private final Observer<? super ObservableHttpResponse<T>> observer;
        private final Observer<T> subject;

        public EntityHandler(Observer<? super ObservableHttpResponse<T>> observer, Observer<T> subject) {
            this.observer = observer;
            this.subject = subject;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            subject.onError(cause);
            observer.onCompleted();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception {
            subject.onNext((T) msg);
            subject.onCompleted();
            observer.onCompleted();
        }
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
    
    private ValidatedFullHttpRequest getHttpRequest(HttpRequest request) throws ClientException {
        ValidatedFullHttpRequest r = null;
        Object entity = request.getEntity();
        String uri = request.getUri().toString();
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
            Serializer serializer = null;
            if (request.getOverrideConfig() != null) {
                serializer = request.getOverrideConfig().getTypedProperty(CommonClientConfigKey.Serializer);
            }
            if (serializer == null) {
                String contentType = getContentType(request.getHeaders());    
                ContentTypeBasedSerializerKey key = new ContentTypeBasedSerializerKey(contentType, entity.getClass());
                serializer = serializationFactory.getSerializer(key);
            }
            if (serializer == null) {
                throw new ClientException("Unable to find serializer");
            }
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            try {
                serializer.serialize(bout, entity);
            } catch (IOException e) {
                throw new ClientException("Error serializing entity in request", e);
            }
            byte[] content = bout.toByteArray();
            ByteBuf buf = Unpooled.wrappedBuffer(content);
            r = new ValidatedFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(request.getVerb().name()), uri, buf);
            r.headers().set(HttpHeaders.Names.CONTENT_LENGTH, content.length);
        } else {
            r = new ValidatedFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(request.getVerb().name()), uri);
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

    
    
    
    
    public <T> Observable<T> execute(final HttpRequest request, TypeDef<T> typeDef) throws Exception {
        ValidatedFullHttpRequest r = getHttpRequest(request);
        if (typeDef.getRawType().isAssignableFrom(HttpResponse.class)) {
            Observable<ObservableHttpResponse<FullHttpResponse>> response = observableClient.execute(r, new FullHttpResponseHandler());
            Observable<FullHttpResponse> fullNettyResponseObservable = response.flatMap(new Func1<ObservableHttpResponse<FullHttpResponse>, Observable<FullHttpResponse>>() {
                @Override
                public Observable<FullHttpResponse> call(
                        ObservableHttpResponse<FullHttpResponse> t1) {
                    return t1.content();
                }
            });
            
            return (Observable<T>) fullNettyResponseObservable.map(new Func1<FullHttpResponse, T>() {

                @Override
                public T call(FullHttpResponse t1) {
                    HttpResponse response = new NettyHttpResponse(t1, t1.content(), serializationFactory, request.getUri());
                    return (T) response;
                }
            });
        }
        
        Observable<ObservableHttpResponse<T>> observableHttpResponse = observableClient.execute(r, new SingleEntityHandler<T>(request, serializationFactory, typeDef));
        return observableHttpResponse.flatMap(new Func1<ObservableHttpResponse<T>, Observable<T>>() {

            @Override
            public Observable<T> call(ObservableHttpResponse<T> t1) {
                return t1.content();
            }
        });
    }    
}
