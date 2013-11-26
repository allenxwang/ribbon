/*
 *
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.client;

import java.io.Closeable;

import com.netflix.serialization.TypeDef;

/**
 * Interface for asynchronous communication client with streaming capability.
 * 
 * @author awang
 *
 * @param <T> Request type
 * @param <S> Response type
 * @param <U> Type of storage used for delivering partial content, for example, {@link ByteBuffer}
 * @param <V> Type of key to find {@link Serializer} and {@link Deserializer} for the content. For example, for HTTP communication,
 *            the key type is {@link ContentTypeBasedSerializerKey}
 */
public interface AsyncClient<T extends ClientRequest, S extends IResponse> extends Closeable {

    /**
     * 
     * @param request
     * @param callback
     * @param type Object to hold the runtime type of the expected entity to be used by entity decoder/deserializer
     * @param <E> Type of the expected entity, can also be type of IResponse to indicate that the caller is interested to
     *           receive the callback on the raw response, or Void to indicate that the caller is not interested to receive
     *           any response from the server
     */
    public <E> ListenableFuture<E> execute(T request, TypeDef<E> type);
}

