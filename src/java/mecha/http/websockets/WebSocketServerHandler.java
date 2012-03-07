package mecha.http.websockets;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.codec.http.websocketx.*;
import org.jboss.netty.logging.*;
import org.jboss.netty.util.*;

import mecha.Mecha;

public class WebSocketServerHandler extends SimpleChannelUpstreamHandler {
    private static final InternalLogger logger = 
        InternalLoggerFactory.getInstance(WebSocketServerHandler.class);
    private static final Logger log = Logger.getLogger(
            WebSocketServerHandler.class.getName());

    final private static String WEBSOCKET_PATH = "/mecha";
    
    private WebSocketServerHandshaker handshaker;
    
    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        super.handleUpstream(ctx, e);
    }
    
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        Mecha.getServer().onOpen(ctx);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        Mecha.getServer().onClose(ctx);
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            handleHttpRequest(ctx, (HttpRequest) msg);
        } else if (msg instanceof WebSocketFrame) {
            handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        }
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest req) throws Exception {
        if (req.getMethod() != GET) {
            sendHttpResponse(ctx, req, new DefaultHttpResponse(HTTP_1_1, FORBIDDEN));
            return;
        }
        
        if (req.getUri().equals("/")) {
            sendHttpResponse(ctx, req, new DefaultHttpResponse(HTTP_1_1, FORBIDDEN));
            return;
        
        } else if (req.getUri().equals("/favicon.ico")) {
            HttpResponse res = new DefaultHttpResponse(HTTP_1_1, NOT_FOUND);
            sendHttpResponse(ctx, req, res);
            return;
        }
        
        WebSocketServerHandshakerFactory wsFactory = 
            new WebSocketServerHandshakerFactory(
                this.getWebSocketLocation(req), 
                null, 
                false);
        this.handshaker = wsFactory.newHandshaker(req);
        if (this.handshaker == null) {
            wsFactory.sendUnsupportedWebSocketVersionResponse(ctx.getChannel());
        } else {
            this.handshaker
                .handshake(ctx.getChannel(), req);
        }
    }

    private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
        
        ctx.setAttachment("websocket");
        
        // Check for closing frame
        if (frame instanceof CloseWebSocketFrame) {
            this.handshaker.close(ctx.getChannel(), (CloseWebSocketFrame) frame);
            return;
        } else if (frame instanceof PingWebSocketFrame) {
            ctx.getChannel().write(new PongWebSocketFrame(frame.getBinaryData()));
            return;
        } else if (!(frame instanceof TextWebSocketFrame)) {
            throw new UnsupportedOperationException(
                String.format("%s frame types not supported", 
                              frame.getClass().getName()));
        }
        
        // REQUEST ***
        String request = ((TextWebSocketFrame) frame).getText();
        Mecha.getServer().onMessage(ctx, request);
        
        /*
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Channel %s received %s", ctx.getChannel().getId(), request));
        }
        
        // RESPONSE ***
        ctx.getChannel().write(new TextWebSocketFrame(request.toUpperCase()));
        */
    }
    
    private void sendHttpResponse(ChannelHandlerContext ctx, HttpRequest req, HttpResponse res) {
        if (res.getStatus().getCode() != 200) {
            res.setContent(
                ChannelBuffers.copiedBuffer(res.getStatus().toString(), 
                                            CharsetUtil.UTF_8));
            setContentLength(res, res.getContent().readableBytes());
        }
        ChannelFuture f = ctx.getChannel().write(res);
        if (!isKeepAlive(req) || res.getStatus().getCode() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
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
    
    private String getWebSocketLocation(HttpRequest req) {
        return "ws://" + req.getHeader(HttpHeaders.Names.HOST) + WEBSOCKET_PATH;
    }
}