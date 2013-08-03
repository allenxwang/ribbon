package com.netflix.loadbalancer;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.netflix.client.ClientConfigBasedRibbonFactory;
import com.netflix.client.DefaultModule;
import com.netflix.client.RibbonFactory;
import com.netflix.client.config.ClientConfigFactory;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;

public class InjectTest {
    static RibbonFactory factory;
    static {
        Injector injector = Guice.createInjector(new DefaultModule());
        factory = injector.getInstance(RibbonFactory.class);
    }
        
    @Test
    public void testLB() {
        assertNotNull(factory);
        assertTrue(factory instanceof ClientConfigBasedRibbonFactory);
        ClientConfigBasedRibbonFactory realFactory = (ClientConfigBasedRibbonFactory) factory;
        ClientConfigFactory clientConfigFactory = realFactory.getConfigFactory();
        assertNotNull(clientConfigFactory);
        assertTrue(clientConfigFactory instanceof DefaultClientConfigImpl.DefaultClientConfigFactory);
        ILoadBalancer lb = factory.getNamedLoadBalancer("default");
        assertTrue(lb instanceof ZoneAwareLoadBalancer);
    }

}
