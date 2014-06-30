package com.netflix.ribbonclientextensions;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.HystrixObservableCommand.Setter;
import com.netflix.ribbonclientextensions.hystrix.FallbackHandler;
import com.netflix.ribbonclientextensions.template.ParsedTemplate;

/**
 * @author awang
 *
 * @param <T> response entity type
 * @param <R> response meta data, e.g. HttpClientResponse
 */
public abstract class RequestTemplate<T, R> {
    
    private final String name;
    private final ResourceGroup<?> group;
    private final Class<? extends T> classType;
    private FallbackHandler<T> fallbackHandler;
    protected final List<CacheProviderWithKeyTemplate<T>> cacheProviders;
    protected ParsedTemplate hystrixCacheKeyTemplate;
    protected Map<String, ParsedTemplate> parsedTemplates;
    protected HystrixObservableCommand.Setter setter;
    protected ResponseValidator<R> validator;

    public static abstract class RequestBuilder<T> {
        public abstract RequestBuilder<T> withRequestProperty(String key, Object value);
        
        public abstract RibbonRequest<T> build();
    }

    public static class CacheProviderWithKeyTemplate<T> {
        private ParsedTemplate keyTemplate;
        private CacheProvider<T> provider;
        public CacheProviderWithKeyTemplate(ParsedTemplate keyTemplate,
                CacheProvider<T> provider) {
            super();
            this.keyTemplate = keyTemplate;
            this.provider = provider;
        }
        public final ParsedTemplate getKeyTemplate() {
            return keyTemplate;
        }
        public final CacheProvider<T> getProvider() {
            return provider;
        }
    }

    protected RequestTemplate(String name,
            ResourceGroup<?> group,
            Class<? extends T> classType) {
        super();
        this.name = name;
        this.group = group;
        this.classType = classType;
        cacheProviders = new LinkedList<CacheProviderWithKeyTemplate<T>>();
        parsedTemplates = new HashMap<String, ParsedTemplate>();
    }
    
    protected ParsedTemplate createParsedTemplate(String template) {
        ParsedTemplate parsedTemplate = parsedTemplates.get(template);
        if (parsedTemplate == null) {
            parsedTemplate = ParsedTemplate.create(template);
            parsedTemplates.put(template, parsedTemplate);
        } 
        return parsedTemplate;
    }

    public final String name() {
        return name;
    }

    public final ResourceGroup<?> group() {
        return group;
    }

    public final Class<? extends T> classType() {
        return classType;
    }


    public abstract RequestBuilder<T> requestBuilder();
    
    public abstract RequestTemplate<T, R> copy(String name);
        
    public RequestTemplate<T, R> withFallbackProvider(FallbackHandler<T> fallbackHandler) {
        this.fallbackHandler = fallbackHandler;
        return this;
    }
    
    public FallbackHandler<T> fallbackHandler() {
        return fallbackHandler;
    }

    public RequestTemplate<T, R> withResponseValidator(ResponseValidator<R> validator) {
        this.validator = validator;
        return this;
    }
        
    /**
     * Calling this method will enable both Hystrix request cache and supplied external cache providers  
     * on the supplied cache key. Caller can explicitly disable Hystrix request cache by calling 
     * {@link #withHystrixProperties(com.netflix.hystrix.HystrixObservableCommand.Setter)}
     *     
     * @param cacheKeyTemplate
     * @return
     */
    public RequestTemplate<T, R> withRequestCacheKey(String cacheKeyTemplate) {
        this.hystrixCacheKeyTemplate = createParsedTemplate(cacheKeyTemplate);
        return this;
    }

    public RequestTemplate<T, R> addCacheProvider(String cacheKeyTemplate, CacheProvider<T> cacheProvider) {
        ParsedTemplate template = createParsedTemplate(cacheKeyTemplate);
        cacheProviders.add(new CacheProviderWithKeyTemplate<T>(template, cacheProvider));
        return this;
    }
    
    public RequestTemplate<T, R> withHystrixProperties(HystrixObservableCommand.Setter setter) {
        this.setter = setter;
        return this;
    }
        
    public ParsedTemplate hystrixCacheKeyTemplate() {
        return hystrixCacheKeyTemplate;
    }
    
    public List<CacheProviderWithKeyTemplate<T>> cacheProviders() {
        return cacheProviders;
    }
    
    public Setter hystrixProperties() {
        return this.setter;
    }
}
