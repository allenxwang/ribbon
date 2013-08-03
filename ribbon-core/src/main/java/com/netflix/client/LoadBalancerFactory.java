package com.netflix.client;

import com.netflix.loadbalancer.ILoadBalancer;

public interface LoadBalancerFactory {
    ILoadBalancer create(String name);
}
