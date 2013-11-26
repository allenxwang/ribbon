package com.netflix.client;

import java.io.InputStream;

public interface ResponseWithEntity extends IResponse {
    public boolean hasEntity();
    
    public InputStream getInputStream() throws ClientException;

}
