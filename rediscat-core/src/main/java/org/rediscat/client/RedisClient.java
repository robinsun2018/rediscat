/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rediscat.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import org.rediscat.api.RFuture;
import org.rediscat.client.handler.RedisChannelInitializer;
import org.rediscat.client.handler.RedisChannelInitializer.Type;
import org.rediscat.misc.RedisURI;
import org.rediscat.misc.RedissonPromise;


import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Low-level Redis client
 * 
 * @author Nikita Koksharov
 *
 */
public final class RedisClient {

    private final AtomicReference<RFuture<InetSocketAddress>> resolvedAddrFuture = new AtomicReference<RFuture<InetSocketAddress>>();
    private final Bootstrap bootstrap;
    private final Bootstrap pubSubBootstrap;
    private final RedisURI uri;
    private InetSocketAddress resolvedAddr;
    private final ChannelGroup channels;

    private ExecutorService executor;
    private final long commandTimeout;
    private RedisClientConfig config;

    private volatile boolean shutdown;


    public boolean isShutdown() {
        return shutdown;
    }

    private RedisClient(RedisClientConfig config) {
        RedisClientConfig copy = new RedisClientConfig(config);

        this.config = copy;
        this.executor = copy.getExecutor();

        uri = copy.getAddress();
        resolvedAddr = copy.getAddr();

        if (resolvedAddr != null) {
            resolvedAddrFuture.set(RedissonPromise.newSucceededFuture(resolvedAddr));
        }

        channels = new DefaultChannelGroup(copy.getGroup().next());
        bootstrap = createBootstrap(copy, RedisChannelInitializer.Type.PLAIN);
        pubSubBootstrap = createBootstrap(copy, Type.PUBSUB);

        this.commandTimeout = copy.getCommandTimeout();
    }

    private Bootstrap createBootstrap(RedisClientConfig config, RedisChannelInitializer.Type type) {
        Bootstrap bootstrap = new Bootstrap()
                .resolver(config.getResolverGroup())
                .channel(config.getSocketChannelClass())
                .group(config.getGroup());

        bootstrap.handler(new RedisChannelInitializer(bootstrap, config, this, channels, type));
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, config.isKeepAlive());
        bootstrap.option(ChannelOption.TCP_NODELAY, config.isTcpNoDelay());
        config.getNettyHook().afterBootstrapInitialization(bootstrap);
        return bootstrap;
    }

    public InetSocketAddress getAddr() {
        return resolvedAddr;
    }

}
