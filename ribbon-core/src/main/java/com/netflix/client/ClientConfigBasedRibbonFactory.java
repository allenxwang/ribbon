package com.netflix.client;

import com.google.inject.Inject;
import com.netflix.client.config.ClientConfigFactory;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractLoadBalancerRule;
import com.netflix.loadbalancer.BaseLoadBalancer;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.IPing;
import com.netflix.loadbalancer.IRule;

public class ClientConfigBasedRibbonFactory implements RibbonFactory {

    @Inject ClientConfigFactory configFactory;
    
    @Override
    public ILoadBalancer getNamedLoadBalancer(String name) {
        IClientConfig config = configFactory.create(name);
        String loadBalancerClassName = String.valueOf(config.getProperty(CommonClientConfigKey.NFLoadBalancerClassName, 
                DefaultClientConfigImpl.DEFAULT_NFLOADBALANCER_CLASSNAME));
        IRule rule = createRule(config);
        IPing ping = createPing(config);
        BaseLoadBalancer lb;
        try {
            lb = (BaseLoadBalancer) Class.forName(loadBalancerClassName).newInstance();
            lb.init(config, rule, ping);
            return lb;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public IRule createRule(IClientConfig config) {
        String loadBalancerClassName = String.valueOf(config.getProperty(CommonClientConfigKey.NFLoadBalancerRuleClassName, 
                DefaultClientConfigImpl.DEFAULT_NFLOADBALANCER_RULE_CLASSNAME));
        AbstractLoadBalancerRule rule;
        try {
            rule = (AbstractLoadBalancerRule) Class.forName(loadBalancerClassName).newInstance();
            rule.initWithNiwsConfig(config);
            return rule;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }        
    }
    
    public ClientConfigFactory getConfigFactory() {
        return configFactory;
    }
    
    public IPing createPing(IClientConfig config) {
        String loadBalancerClassName = String.valueOf(config.getProperty(CommonClientConfigKey.NFLoadBalancerPingClassName, 
                DefaultClientConfigImpl.DEFAULT_NFLOADBALANCER_PING_CLASSNAME));
        IPing ping;
        try {
            ping = (IPing) Class.forName(loadBalancerClassName).newInstance();
            return ping;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
