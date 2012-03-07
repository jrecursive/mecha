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