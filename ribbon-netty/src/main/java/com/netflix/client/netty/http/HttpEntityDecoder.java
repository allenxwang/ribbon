package com.netflix.client.netty.http;

import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;

import java.util.List;

import com.netflix.serialization.ContentTypeBasedSerializerKey;
import com.netflix.serialization.Deserializer;
import com.netflix.serialization.SerializationFactory;
import com.netflix.serialization.TypeDef;
import com.sun.xml.internal.messaging.saaj.packaging.mime.Header;

public class HttpEntityDecoder<T> extends MessageToMessageDecoder<FullHttpResponse> {

    private TypeDef<T> type;
    private SerializationFactory<ContentTypeBasedSerializerKey> serializationFactory;
    
    public HttpEntityDecoder(SerializationFactory<ContentTypeBasedSerializerKey> serializationFactory, TypeDef<T> type) {
        this.serializationFactory = serializationFactory;
        this.type = type;
    }
    
    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpResponse msg,
            List<Object> out) throws Exception {
        String contentType = msg.headers().get(HttpHeaders.Names.CONTENT_TYPE);
        Deserializer deserializer = serializationFactory.getDeserializer(new ContentTypeBasedSerializerKey(contentType, type));
        int statusCode = msg.getStatus().code();
        String reason = msg.getStatus().reasonPhrase();
        if (statusCode >= 200 && statusCode < 300) {
            T obj = deserializer.deserialize(new ByteBufInputStream(msg.content()), type);
            if (obj != null) {
                out.add(obj);
            }            
        } else {
            throw new Exception(reason);
        }
    }
}
