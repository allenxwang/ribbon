package com.netflix.ribbonclientextensions.http;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;

import com.netflix.client.netty.LoadBalancingRxClient;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.HystrixObservableCommand.Setter;
import com.netflix.ribbonclientextensions.CacheProvider;
import com.netflix.ribbonclientextensions.RequestTemplate;
import com.netflix.ribbonclientextensions.ResponseValidator;
import com.netflix.ribbonclientextensions.hystrix.FallbackHandler;
import com.netflix.ribbonclientextensions.template.ParsedTemplate;

public class HttpRequestTemplate<T> extends RequestTemplate<T, HttpClientResponse<ByteBuf>> {

    private final HttpClient<ByteBuf, ByteBuf> client;
    private final String clientName;
    private final int maxResponseTime;
    private FallbackHandler<T> fallbackHandler;
    private ParsedTemplate parsedUriTemplate;
    private HttpMethod method;
    private final int concurrentRequestLimit;
    private final HttpHeaders headers;
    
    
    public HttpRequestTemplate(String name, HttpResourceGroup group, Class<? extends T> classType) {
        super(name, group, classType);
        this.client = group.getClient();
        if (client instanceof LoadBalancingRxClient) {
            LoadBalancingRxClient<?, ? ,?> ribbonClient = (LoadBalancingRxClient<?, ? ,?>) client;
            maxResponseTime = ribbonClient.getResponseTimeOut();
            clientName = ribbonClient.getName();
            concurrentRequestLimit = ribbonClient.getMaxConcurrentRequests();
        } else {
            clientName = client.getClass().getName();
            maxResponseTime = -1;
            concurrentRequestLimit = -1;
        }
        method = HttpMethod.GET;
        headers = new DefaultHttpHeaders();
        headers.add(group.getHeaders());
    }
    
    @Override
    public HttpRequestTemplate<T> withFallbackProvider(FallbackHandler<T> fallbackHandler) {
        return (HttpRequestTemplate<T>) super.withFallbackProvider(fallbackHandler);
    }

    @Override
    public HttpRequestBuilder<T> requestBuilder() {
        if (setter == null) {
            setter = HystrixObservableCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(clientName))
                    .andCommandKey(HystrixCommandKey.Factory.asKey(name()));
            HystrixCommandProperties.Setter commandProps = HystrixCommandProperties.Setter();
            if (maxResponseTime > 0) {
               commandProps.withExecutionIsolationThreadTimeoutInMilliseconds(maxResponseTime);
            }
            if (concurrentRequestLimit > 0) {
                commandProps.withExecutionIsolationSemaphoreMaxConcurrentRequests(concurrentRequestLimit);                
            }
            setter.andCommandPropertiesDefaults(commandProps);
        }
        return new HttpRequestBuilder<T>(this);
    }
    
    public HttpRequestTemplate<T> withMethod(String method) {
        this.method = HttpMethod.valueOf(method);
        return this;
    }
    
    
    public HttpRequestTemplate<T> withUriTemplate(String uri) {
        this.parsedUriTemplate = createParsedTemplate(uri);
        return this;
    }
    
    public HttpRequestTemplate<T> withHeader(String name, String value) {
        headers.add(name, value);
        return this;
    }    
    
    @Override
    public HttpRequestTemplate<T> withRequestCacheKey(
            String cacheKeyTemplate) {
        this.hystrixCacheKeyTemplate = createParsedTemplate(cacheKeyTemplate);
        return this;
    }

    @Override
    public HttpRequestTemplate<T> addCacheProvider(String keyTemplate, 
            CacheProvider<T> cacheProvider) {
        return (HttpRequestTemplate<T>) super.addCacheProvider(keyTemplate, cacheProvider);
    }
        
    
    ResponseValidator<HttpClientResponse<ByteBuf>> responseValidator() {
        return validator;
    }
        
    ParsedTemplate uriTemplate() {
        return parsedUriTemplate;
    }
    
    HttpMethod method() {
        return method;
    }
        
    HttpHeaders getHeaders() {
        return this.headers;
    }
        
    @Override
    public HttpRequestTemplate<T> withResponseValidator(
            ResponseValidator<HttpClientResponse<ByteBuf>> validator) {
        return (HttpRequestTemplate<T>) super.withResponseValidator(validator);
    }

    @Override
    public HttpRequestTemplate<T> copy(String name) {
        HttpRequestTemplate<T> newTemplate = new HttpRequestTemplate<T>(name, (HttpResourceGroup) this.group(), this.classType());
        newTemplate.cacheProviders.addAll(this.cacheProviders);
        newTemplate.method = this.method;
        newTemplate.headers.add(this.headers);
        newTemplate.parsedTemplates.putAll(this.parsedTemplates);
        newTemplate.parsedUriTemplate = this.parsedUriTemplate;
        newTemplate.setter = setter;
        newTemplate.fallbackHandler = this.fallbackHandler;
        newTemplate.validator = this.validator;
        newTemplate.hystrixCacheKeyTemplate = this.hystrixCacheKeyTemplate;
        return newTemplate;
    }

    @Override
    public HttpRequestTemplate<T> withHystrixProperties(
            Setter propertiesSetter) {
        return (HttpRequestTemplate<T>) super.withHystrixProperties(propertiesSetter); 
    }
        
    HttpClient<ByteBuf, ByteBuf> getClient() {
        return this.client;
    }
}

