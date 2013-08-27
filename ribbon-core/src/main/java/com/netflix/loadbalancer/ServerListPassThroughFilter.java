package com.netflix.loadbalancer;

import java.util.List;

public class ServerListPassThroughFilter<T extends Server> extends AbstractServerListFilter<T> {

    @Override
    public List<T> getFilteredListOfServers(List<T> servers) {
        return servers;
    }

}
