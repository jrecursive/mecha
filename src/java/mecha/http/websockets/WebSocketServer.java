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

package mecha.http.websockets;

import java.net.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import mecha.Mecha;

public class WebSocketServer {
    final private static Logger log = 
        Logger.getLogger(WebSocketServer.class.getName());
    
    final private String addr;
    final private int port;
    
    public WebSocketServer() throws Exception {
        port = Mecha.getConfig().getInt("websocket-port");
        addr = Mecha.getConfig().getString("server-addr");
    }
    
    public void start() {
        ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new WebSocketServerPipelineFactory());
        bootstrap.bind(new InetSocketAddress(addr, port));
        log.info("* websocket server started (port " + port + ")");
    }
}