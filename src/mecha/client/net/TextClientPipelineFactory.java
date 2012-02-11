package mecha.client.net;

import static org.jboss.netty.channel.Channels.*;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import mecha.Mecha;
import mecha.client.*;

public class TextClientPipelineFactory implements
        ChannelPipelineFactory {
    
    final private MechaClientHandler handler;

    public TextClientPipelineFactory(MechaClientHandler handler) {
        this.handler = handler;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = pipeline();
        pipeline.addLast("framer", new DelimiterBasedFrameDecoder(
                Mecha.getConfig().<Integer>get("netty-frame-size"), 
                Delimiters.lineDelimiter()));
        pipeline.addLast("decoder", new StringDecoder());
        pipeline.addLast("encoder", new StringEncoder());
        pipeline.addLast("handler", new TextClientHandler(handler));
        return pipeline;
    }
}