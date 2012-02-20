package mecha.vm.channels;

import java.util.*;
import java.util.logging.*;

import mecha.Mecha;
import mecha.json.*;

/*
 * messaging channel
*/
public class PubChannel {
    final private static Logger log = 
        Logger.getLogger(PubChannel.class.getName());

    public String name;
    public Set<ChannelConsumer> members;
    
    public PubChannel(String name) {
        this.name = name;
        members = Collections.synchronizedSet(new HashSet());
    }
    
    public void addMember(ChannelConsumer cc) {
        members.add(cc);
    }
    
    public void removeMember(ChannelConsumer cc) {
        members.remove(cc);
        if (members.size() == 0) {
            Mecha.getChannels().destroyChannel(name);
        }
    }
    
    public void removeAllMembers() {
        members.clear();
        Mecha.getChannels().destroyChannel(name);
    }
    
    public Set<ChannelConsumer> getMembers() {
        return members;
    }
    
    public void send(String message) throws Exception {
        for(ChannelConsumer cc : members) {
            cc.onMessage(name, message);
        }
    }
    
    public void send(JSONObject message) throws Exception {
        for(ChannelConsumer cc : members) {
            cc.onMessage(name, message);
        }
    }

    public void send(byte[] message) throws Exception {
        for(ChannelConsumer cc : members) {
            cc.onMessage(name, message);
        }
    }
    
    public String getName() {
        return name;
    }

}
