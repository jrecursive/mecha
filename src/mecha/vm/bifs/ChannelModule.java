package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;

import mecha.Mecha;
import mecha.vm.channels.*;
import mecha.json.*;
import mecha.vm.*;

public class ChannelModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(ChannelModule.class.getName());

    public ChannelModule() throws Exception { super(); }
        
    public void moduleLoad() throws Exception { }
    
    public void moduleUnload() throws Exception { }
    
    public class Subscribe extends MVMFunction {
        public Subscribe(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
        
        public void start(JSONObject msg) throws Exception {
            String channel = getConfig().getString("channel");
            if (channel.endsWith("*")) {
                channel = channel.substring(0, channel.length()-1);
                for(String channelName : Mecha.getChannels().getChannelNames()) {
                    if (channelName.startsWith(channel)) {
                        PubChannel pubChannel = 
                            Mecha.getChannels().getChannel(channelName);
                        pubChannel.addMember(getContext().getClient());
                        getContext().getClient().addSubscription(channelName);
                    }
                }
            } else {
                PubChannel pubChannel = 
                    Mecha.getChannels().getOrCreateChannel(channel);
                pubChannel.addMember(getContext().getClient());
                getContext().getClient().addSubscription(channel);
            }
            broadcastDone();
        }
    }
    
    public class Unsubscribe extends MVMFunction {
        public Unsubscribe(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            String channel = config.getString("channel");
            PubChannel pubChannel = 
                Mecha.getChannels().getOrCreateChannel(channel);
            if (pubChannel == null) {
                return;
            } else {
                pubChannel.removeMember(ctx.getClient());
                ctx.getClient().removeSubscription(channel);
            }
        }
    }

    public class Publish extends MVMFunction {
        public Publish(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            String channel = config.getString("channel");
            JSONObject data = config.getJSONObject("data");
            PubChannel pubChannel = 
                Mecha.getChannels().getChannel(channel);
            if (pubChannel == null) {
                return;
            } else {
                pubChannel.send(data);
            }
        }
    }
}