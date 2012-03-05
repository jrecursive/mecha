package mecha.vm.channels;

import java.util.*;

import mecha.json.*;

public interface ChannelConsumer {
    
    public void onMessage(String channel, String message) throws Exception;
    
    public void onMessage(String channel, JSONObject message) throws Exception;
    
    public void onMessage(String channel, byte[] message) throws Exception;
    
}