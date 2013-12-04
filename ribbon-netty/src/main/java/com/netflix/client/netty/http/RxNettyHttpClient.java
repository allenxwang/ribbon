package com.netflix.client.netty.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import com.netflix.client.ClientException;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.http.HttpRequest;
import com.netflix.serialization.ContentTypeBasedSerializerKey;
import com.netflix.serialization.JacksonSerializationFactory;
import com.netflix.serialization.SerializationFactory;
import com.netflix.serialization.Serializer;
import com.netflix.serialization.TypeDef;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringEncoder;
import rx.Observable;
import rx.Observer;
import rx.netty.protocol.http.HttpMessageObserver;
import rx.netty.protocol.http.HttpProtocolHandlerAdapter;
import rx.netty.protocol.http.ObservableHttpClient;
import rx.netty.protocol.http.ObservableHttpResponse;
import rx.netty.protocol.http.ValidatedFullHttpRequest;
import rx.util.functions.Func1;

public class RxNettyHttpClient {

    private ObservableHttpClient observableClient;
    private SerializationFactory<ContentTypeBasedSerializerKey> serializationFactory;
    
    public RxNettyHttpClient() {
        this.observableClient = ObservableHttpClient.newBuilder().build(new NioEventLoopGroup());
        this.serializationFactory = new JacksonSerializationFactory();
    }
    
    private static class SingleEntityHandler<T> extends HttpProtocolHandlerAdapter<T> {

        private HttpEntityDecoder<T> decoder;
        
        private SingleEntityHandler(HttpRequest request, SerializationFactory<ContentTypeBasedSerializerKey> serializationFactory, TypeDef<T> typeDef) {
            decoder = new HttpEntityDecoder<T>(serializationFactory, request, typeDef);
        }
        
        @Override
        public void configure(ChannelPipeline pipeline) {
            super.configure(pipeline);
            pipeline.addAfter("http-aggregator", "entity-decoder", decoder);
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
    
    private io.netty.handler.codec.http.FullHttpRequest getHttpRequest(HttpRequest request) throws ClientException {
        io.netty.handler.codec.http.FullHttpRequest r = null;
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

    private static class SingleEntityHttpMessageObserver<T> extends HttpMessageObserver<T> {

        
        public SingleEntityHttpMessageObserver(
                Observer<? super ObservableHttpResponse<T>> observer,
                ObservableHttpResponse<T> response) {
            super(observer, response);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception {
            super.channelRead(ctx, msg);
            
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx)
                throws Exception {
            // TODO Auto-generated method stub
            super.channelUnregistered(ctx);
        }
        
        
        
    }
    
    
    public <T> Observable<T> execute(HttpRequest request, TypeDef<T> typeDef) throws Exception {
        io.netty.handler.codec.http.FullHttpRequest r = getHttpRequest(request);
        ValidatedFullHttpRequest newRequest = new ValidatedFullHttpRequest(r.getProtocolVersion(), r.getMethod(), request.getUri(), r.content());
        Observable<ObservableHttpResponse<T>> observableHttpResponse = observableClient.execute(newRequest, new SingleEntityHandler<T>(request, serializationFactory, typeDef));
        return observableHttpResponse.flatMap(new Func1<ObservableHttpResponse<T>, Observable<T>>() {

            @Override
            public Observable<T> call(ObservableHttpResponse<T> t1) {
                return t1.content();
            }
        });
    }    
}
