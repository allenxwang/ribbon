package com.netflix.client.config;

public interface ClientConfigFactory {

    IClientConfig create(String clientName);
}
