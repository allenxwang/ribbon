package com.netflix.ribbonclientextensions.http;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;

import java.util.List;
import java.util.Map;

import rx.Observable;
import rx.functions.Func1;

import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.ribbonclientextensions.ResponseValidator;
import com.netflix.ribbonclientextensions.ServerError;
import com.netflix.ribbonclientextensions.UnsuccessfulResponseException;
import com.netflix.ribbonclientextensions.http.HttpRequest.CacheProviderWithKey;
import com.netflix.ribbonclientextensions.hystrix.FallbackHandler;

class RibbonHystrixObservableCommand<T> extends HystrixObservableCommand<T> {

    private final HttpClient<ByteBuf, ByteBuf> httpClient;
    private final HttpClientRequest<ByteBuf> httpRequest;
    private final String hystrixCacheKey;
    private final List<CacheProviderWithKey<T>> cacheProviders;
    private final Map<String, Object> requestProperties;
    private final FallbackHandler<T> fallbackHandler;
    private final Class<? extends T> classType;
    private final ResponseValidator<HttpClientResponse<ByteBuf>> validator;

    RibbonHystrixObservableCommand(HttpClient<ByteBuf, ByteBuf> httpClient,
            HttpClientRequest<ByteBuf> httpRequest, String hystrixCacheKey,
            List<CacheProviderWithKey<T>> cacheProviders,
            Map<String, Object> requestProperties,
            FallbackHandler<T> fallbackHandler,
            ResponseValidator<HttpClientResponse<ByteBuf>> validator,
            Class<? extends T> classType,
            HystrixObservableCommand.Setter setter) {
        super(setter);
        this.httpClient = httpClient;
        this.fallbackHandler = fallbackHandler;
        this.validator = validator;
        this.httpRequest = httpRequest;
        this.hystrixCacheKey = hystrixCacheKey;
        this.cacheProviders = cacheProviders;
        this.classType = classType;
        this.requestProperties = requestProperties;
    }

    @Override
    protected String getCacheKey() {
        if (hystrixCacheKey == null) {
            return super.getCacheKey();
        } else {
            return hystrixCacheKey;
        }
    }
    
    @Override
    protected Observable<T> getFallback() {
        if (fallbackHandler == null) {
            return super.getFallback();
        } else {
            return fallbackHandler.getFallback(this, this.requestProperties);
        }
    }

    @Override
    protected Observable<T> run() {
        Observable<T> cached = null;
        for (CacheProviderWithKey<T> provider: cacheProviders) { 
            Observable<T> fromTheProvider = provider.getCacheProvider().get(provider.getKey(), this.requestProperties);
            if (cached == null) {
                cached = fromTheProvider;
            } else {
                cached = cached.onErrorResumeNext(fromTheProvider);
            }
        }
        Observable<HttpClientResponse<ByteBuf>> httpResponseObservable = httpClient.submit(httpRequest);
        if (this.validator != null) {
            httpResponseObservable = httpResponseObservable.map(new Func1<HttpClientResponse<ByteBuf>, HttpClientResponse<ByteBuf>>(){
                @Override
                public HttpClientResponse<ByteBuf> call(HttpClientResponse<ByteBuf> t1) {
                    try {
                        validator.validate(t1);
                    } catch (UnsuccessfulResponseException e) {
                        throw new HystrixBadRequestException("Unsuccessful response", e);
                    } catch (ServerError e) {
                        throw new RuntimeException(e);
                    }
                    return t1;
                }
            });
        }
        Observable<T> httpEntities = httpResponseObservable.flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<T>>() {
                    @Override
                    public Observable<T> call(HttpClientResponse<ByteBuf> t1) {
                        return t1.getContent().map(new Func1<ByteBuf, T>(){
                            @Override
                            public T call(ByteBuf t1) {
                                return classType.cast(t1);
                            }
                            
                        });
                    }
                });
        if (cached != null) {
            return cached.onErrorResumeNext(httpEntities);
        } else {
            return httpEntities;
        }
    }
}
