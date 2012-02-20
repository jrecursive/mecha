package mecha.vm.channels;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.monitoring.*;

public class Channels {
    final private static Logger log = 
        Logger.getLogger(Channels.class.getName());
    
    final private Rates rates;
    
    /*
     * map: names -> channels
    */
    final private Map<String, PubChannel> channelMap;
    
    final private Thread janitorThread;
    
    public Channels() throws Exception {
        channelMap = new ConcurrentHashMap<String, PubChannel>();
        rates = new Rates();
        janitorThread = new Thread(new Runnable() {
            public void run() {
                while(true) {
                    try {
                        Set<String> deadChannels = new HashSet<String>();
                        for(PubChannel channel : channelMap.values()) {
                            if (channel.getMembers().size() == 0) {
                                log.info("Destroying channel " + channel.getName());
                                deadChannels.add(channel.getName());
                            }
                        }
                        for(String channelName : deadChannels) {
                            destroyChannel(channelName);
                            try {
                                destroyChannel(channelName + "-c");
                            } catch (Exception ex) {
                                // try to destroy them; -c is the one likely to fail.
                            }
                        }
                        Thread.sleep(60000);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        Mecha.getMonitoring().error("mecha.vm.channels", ex);
                    }
                }
            }
        });
    }
    
    public void start() throws Exception {
        Mecha.getMonitoring().addMonitoredRates(rates);
        
        log.info("* starting channel janitor");
        janitorThread.start();
    }
    
    public PubChannel getOrCreateChannel(String channel) throws Exception {
        PubChannel pubChannel = channelMap.get(channel);
        if (pubChannel == null) {
            rates.add("mecha.vm.channels.create");
            pubChannel = new PubChannel(channel);
            channelMap.put(channel, pubChannel);
        }
        return pubChannel;
    }
    
    public PubChannel getChannel(String channel) {
        return channelMap.get(channel);
    }
    
    public boolean channelExists(String channel) {
        return channelMap.containsKey(channel);
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
    
    public void destroyChannel(String channel) {
        rates.add("mecha.vm.channels.destroy");
        channelMap.remove(channel);
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