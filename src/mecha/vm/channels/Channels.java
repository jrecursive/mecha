package mecha.vm.channels;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.json.*;

public class Channels {
    final private static Logger log = 
        Logger.getLogger(Channels.class.getName());

    /*
     * map: names -> channels
    */
    final private Map<String, PubChannel> channelMap;

    public Channels() throws Exception {
        channelMap = new ConcurrentHashMap<String, PubChannel>();
    }
    
    public PubChannel getOrCreateChannel(String channel) throws Exception {
        PubChannel pubChannel = channelMap.get(channel);
        if (pubChannel == null) {
            pubChannel = new PubChannel(channel);
            channelMap.put(channel, pubChannel);
        }
        return pubChannel;
    }
    
    public PubChannel getChannel(String channel) {
        return channelMap.get(channel);
    }
    
    public Set<String> getChannelNames() {
        return channelMap.keySet();
    }
    
    public Collection<PubChannel> getAllChannels() {
        return channelMap.values();
    }
    
    public Set<Map.Entry<String, PubChannel>> getChannelSet() {
        return channelMap.entrySet();
    }
    
    /**
     * Sends a message to all subscribers of specified channel
     *
     * @param chan  the channel to send msg to
     * @param msg   the message to send to all subscribers of chan
     *
     */
    private void send(String channel, String msg) throws Exception {
        PubChannel pubChannel = channelMap.get(channel);
        pubChannel.send(msg);
    }
    
    public void send(String channel, JSONObject msg) throws Exception {
        PubChannel pubChannel = channelMap.get(channel);
        pubChannel.send(msg);
    }
    
    public void send(String channel, byte[] msg) throws Exception {
        PubChannel pubChannel = channelMap.get(channel);
        pubChannel.send(msg);
    }
}