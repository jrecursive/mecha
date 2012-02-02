package mecha.client.net;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import mecha.client.*;

public class TextClient {

    private final String host;
    private final int port;
    private final String password;
    private final MechaClientHandler handler;
    private final ClientBootstrap bootstrap;
    private final ChannelFuture future;
    private final Channel channel;
    
    public TextClient(String host, 
                        int port, 
                        String password, 
                        MechaClientHandler handler) throws Exception {
        this.host = host;
        this.port = port;
        this.handler = handler;
        this.password = password;
        bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new TextClientPipelineFactory(handler));
        future = bootstrap.connect(new InetSocketAddress(host, port));
        channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess()) {
            future.getCause().printStackTrace();
            bootstrap.releaseExternalResources();
            throw new Exception("Unable to connect " + host + ":" + port);
        }
        System.out.println("writing password .. " + password);
        channel.write("auth " + password + "\n");
    }
    
    public Channel getChannel() {
        return channel;
    }
    
    public void send(String msg) throws Exception {
        channel.write(msg + "\n");
    }
}