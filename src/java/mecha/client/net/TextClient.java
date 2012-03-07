/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package mecha.client.net;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import mecha.client.*;

public class TextClient {
    final private static Logger log = 
        Logger.getLogger(TextClient.class.getName());

    private final String host;
    private final int port;
    private final String password;
    private final MechaClientHandler handler;
    private final ClientBootstrap bootstrap;
    private ChannelFuture future = null;
    private Channel channel = null;
    private final Semaphore ready;
    
    public TextClient(final String host, 
                      final int port, 
                      final String password, 
                      final MechaClientHandler handler) throws Exception {
        this.host = host;
        this.port = port;
        this.handler = handler;
        this.password = password;
        ready = new Semaphore(1);
        ready.acquire();
        bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.sendBufferSize", 1048576);
        bootstrap.setOption("child.receiveBufferSize", 1048576);
        bootstrap.setOption("writeBufferLowWaterMark", 32 * 1024);
        bootstrap.setOption("writeBufferHighWaterMark", 64 * 1024);
        bootstrap.setPipelineFactory(new TextClientPipelineFactory(handler));
        bootstrap.connect(new InetSocketAddress(host, port)).addListener(
            new ChannelFutureListener() {
                public void operationComplete(ChannelFuture channelFuture) {
                    future = channelFuture;
                    if (!future.isSuccess()) {
                        future.getCause().printStackTrace();
                        bootstrap.releaseExternalResources();
                    }
                    channel = future.getChannel();
                    channel.write("auth " + password + "\n");
                    ready.release();
                }
            }
        );
        // TODO: timeout
        ready.acquire();
        if (channel == null) {
            throw new Exception("Unable to connect " + host + ":" + port);
        }
    }
    
    public Channel getChannel() {
        return channel;
    }
    
    public void close() throws Exception {
        channel.close();
    }
    
    public void send(String msg) throws Exception {
        channel.write(msg + "\n");
    }
    
    public String getHost() {
        return host;
    }
    
}