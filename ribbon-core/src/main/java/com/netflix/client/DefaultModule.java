package com.netflix.client;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.netflix.client.config.ClientConfigFactory;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AvailabilityFilteringRule;
import com.netflix.loadbalancer.DummyPing;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.IPing;
import com.netflix.loadbalancer.IRule;
import com.netflix.loadbalancer.ZoneAwareLoadBalancer;

public class DefaultModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ClientConfigFactory.class).to(DefaultClientConfigImpl.DefaultClientConfigFactory.class);
        bind(RibbonFactory.class).to(ClientConfigBasedRibbonFactory.class);
    }
}
