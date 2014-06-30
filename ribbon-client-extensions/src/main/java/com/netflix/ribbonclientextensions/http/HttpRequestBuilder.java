package com.netflix.ribbonclientextensions.http;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.RawContentSource;
import io.reactivex.netty.protocol.http.client.RepeatableContentHttpRequest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.ribbonclientextensions.RequestTemplate.CacheProviderWithKeyTemplate;
import com.netflix.ribbonclientextensions.RequestTemplate.RequestBuilder;
import com.netflix.ribbonclientextensions.RibbonRequest;
import com.netflix.ribbonclientextensions.template.ParsedTemplate;
import com.netflix.ribbonclientextensions.template.TemplateParser;
import com.netflix.ribbonclientextensions.template.TemplateParsingException;

public class HttpRequestBuilder<T> extends RequestBuilder<T> {

    private final HttpRequestTemplate<T> requestTemplate;
    private final Map<String, Object> vars;
    private final ParsedTemplate parsedUriTemplate;
    private RawContentSource<?> rawContentSource;
    
    HttpRequestBuilder(HttpRequestTemplate<T> requestTemplate) {
        this.requestTemplate = requestTemplate;
        this.parsedUriTemplate = requestTemplate.uriTemplate();
        vars = new ConcurrentHashMap<String, Object>();
    }
    
    @Override
    public HttpRequestBuilder<T> withRequestProperty(
            String key, Object value) {
        vars.put(key, value.toString());
        return this;
    }
        
    public HttpRequestBuilder<T> withRawContentSource(RawContentSource<?> raw) {
        this.rawContentSource = raw;
        return this;
    }

    @Override
    public RibbonRequest<T> build() {
        try {
            return new HttpRequest<T>(this);
        } catch (TemplateParsingException e) {
            throw new IllegalArgumentException(e);
        }
    }
        
    HttpClientRequest<ByteBuf> createClientRequest() {
        String uri;
        try {
            uri = TemplateParser.toData(vars, parsedUriTemplate.getTemplate(), parsedUriTemplate.getParsed());
        } catch (TemplateParsingException e) {
            throw new HystrixBadRequestException("Problem parsing the URI template", e);
        }
        HttpClientRequest<ByteBuf> request =  HttpClientRequest.create(requestTemplate.method(), uri);
        for (Map.Entry<String, String> entry: requestTemplate.getHeaders().entries()) {
            request.withHeader(entry.getKey(), entry.getValue());
        }
        if (rawContentSource != null) {
            request.withRawContentSource(rawContentSource);
        }
        return new RepeatableContentHttpRequest<ByteBuf>(request);
    }
    
    String hystrixCacheKey() throws TemplateParsingException {
        if (requestTemplate.hystrixCacheKeyTemplate() == null) {
            return null;
        }
        return TemplateParser.toData(vars, requestTemplate.hystrixCacheKeyTemplate());
    }
     
    Map<String, Object> requestProperties() {
        return vars;
    }
    
    List<CacheProviderWithKeyTemplate<T>> cacheProviders() {
        return requestTemplate.cacheProviders();
    }
    
    HttpRequestTemplate<T> template() {
        return requestTemplate;
    }
}
