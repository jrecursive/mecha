package mecha.server.net;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class MechaServer implements Runnable {
    private final String addr;
    private final int port;

    public MechaServer(String addr, int port) {
        this.addr = addr;
        this.port = port;
    }

    public void run() {
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.sendBufferSize", 1048576);
        bootstrap.setOption("child.receiveBufferSize", 1048576);
        bootstrap.setOption("writeBufferLowWaterMark", 32 * 1024);
        bootstrap.setOption("writeBufferHighWaterMark", 64 * 1024);
        bootstrap.setPipelineFactory(new MechaServerPipelineFactory());
        bootstrap.bind(new InetSocketAddress(addr, port));
    }
}