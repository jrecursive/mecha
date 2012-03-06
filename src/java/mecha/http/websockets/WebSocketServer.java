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
        addr = Mecha.getConfig().getString("http-addr");
    }
    
    public void start() {
        ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new WebSocketServerPipelineFactory());
        bootstrap.bind(new InetSocketAddress(addr, port));
        log.info("* websocket server started (port " + port + ")");
    }
}