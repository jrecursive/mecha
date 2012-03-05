package mecha.client.net;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import mecha.client.*;

public class TextClientHandler extends SimpleChannelUpstreamHandler {
    private static final Logger log = Logger.getLogger(
            TextClientHandler.class.getName());

    final private MechaClientHandler handler;

    public TextClientHandler(MechaClientHandler handler) {
        this.handler = handler;
    }

    @Override
    public void handleUpstream(
            ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        super.handleUpstream(ctx, e);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        handler.onOpen();
    }
    
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        handler.onClose();
    }
    
    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
        String msg = (String) e.getMessage();
        handler.onMessage(msg);
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
        /*
         * Any exception at this point should just kill the channel
         *  which will cause a cascade of cleanup activities; almost
         *  all non-static functionality (e.g., user, data driven)
         *  relies on a connection -- if there is a problem, it should
         *  all be dumped ASAP.
        */
        /*
        log.log(
                Level.WARNING,
                "Unexpected exception from downstream.",
                e.getCause());
        */
        e.getChannel().close();
    }
}