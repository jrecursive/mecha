package mecha.client.net;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean ready;
    
    public TextClient(final String host, 
                      final int port, 
                      final String password, 
                      final MechaClientHandler handler) throws Exception {
        this.host = host;
        this.port = port;
        this.handler = handler;
        this.password = password;
        ready = new AtomicBoolean(false);
        bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
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
                    System.out.println("writing password .. " + password);
                    channel.write("auth " + password + "\n");
                    ready.set(true);
                }
            }
        );
        
        log.info("Waiting for ready state...");
        while(!ready.get()) {
            Thread.sleep(5);
        }
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
}