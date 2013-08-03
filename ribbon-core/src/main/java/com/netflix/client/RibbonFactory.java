package com.netflix.client;

import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.IPing;
import com.netflix.loadbalancer.IRule;

public interface RibbonFactory {
    ILoadBalancer getNamedLoadBalancer(String clientName);
    
    IRule createRule(String clientName);
    
    IPing createPing(String clientName);
    
    
}
