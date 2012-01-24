package mecha.vm.channels;

import java.util.*;

import org.json.*;

public interface ChannelConsumer {
    
    public void onMessage(String message) throws Exception;
    
    public void onMessage(JSONObject message) throws Exception;
    
    public void onMessage(byte[] message) throws Exception;
    
}