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
package org.rediscat.misc;

import io.netty.util.NetUtil;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @author robinsun
 */
public class RedisURI {

    private final String host;
    private final int port;

    public RedisURI(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public RedisURI(String uri) {
        if (!uri.startsWith("redis://")) {
            throw new IllegalArgumentException("Redis url should start with redis://");
        }
        
        String urlHost = uri.replaceFirst("redis://", "http://");
        String ipV6Host = uri.substring(uri.indexOf("://")+3, uri.lastIndexOf(":"));
        if (ipV6Host.contains("@")) {
            ipV6Host = ipV6Host.split("@")[1];
        }
        if (ipV6Host.contains(":")) {
            urlHost = urlHost.replace(ipV6Host, "[" + ipV6Host + "]");
        }

        try {
            URL url = new URL(urlHost);
            host = url.getHost();
            port = url.getPort();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isIP() {
        return NetUtil.createByteArrayFromIpAddressString(host) != null;
    }

    private static String trimIpv6Brackets(String host) {
        if (host.startsWith("[") && host.endsWith("]")) {
            return host.substring(1, host.length() - 1);
        }
        return host;
    }
    
    public static boolean compare(InetSocketAddress entryAddr, RedisURI addr) {
        if (((entryAddr.getHostName() != null && entryAddr.getHostName().equals(trimIpv6Brackets(addr.getHost())))
                || entryAddr.getAddress().getHostAddress().equals(trimIpv6Brackets(addr.getHost())))
                && entryAddr.getPort() == addr.getPort()) {
            return true;
        }
        return false;
    }

    @Override
    @SuppressWarnings("AvoidInlineConditionals")
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RedisURI other = (RedisURI) obj;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (port != other.port)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "http://" + trimIpv6Brackets(host) + ":" + port;
    }
    
}
