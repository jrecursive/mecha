package mecha.server;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.webbitserver.*;
import org.webbitserver.handler.*;
import org.webbitserver.handler.exceptions.*;

import mecha.Mecha;
import mecha.util.*;
import mecha.vm.*;
import mecha.vm.channels.*;

import org.json.*;

/*
 * client record
*/
public class Client implements ChannelConsumer {
    final private static Logger log = 
        Logger.getLogger(Client.class.getName());

    /*
     * for Server
    */
    final private ConcurrentHashMap<String, String> state;
    private boolean authorized = false;
    
    /*
     * messaging via mecha.vm.channels.Channels
    */
    final private Set<String> subscriptions;
    final private WeakReference<WebSocketConnection> connection;
    final private PubChannel clientChannel;
    
    /*
     * Mecha VM
    */
    final private MVMContext ctx;
    
    /*
     * per-client identifiers
    */
    final private String id;
    
    public Client(WebSocketConnection connection) throws Exception {
        id = "socket-" +
             HashUtils.sha1(
                UUID.randomUUID() + "-" +
                System.currentTimeMillis()
             );
        log.info("new client: " + id);
    
        state = new ConcurrentHashMap<String, String>();
        this.connection = new WeakReference<WebSocketConnection>(connection);
        subscriptions = Collections.synchronizedSet(new HashSet());
        
        /*
         * MVMContext keeps WeakReference<Client>
        */
        ctx = new MVMContext(this);
        
        /*
         * subscribe to own channel
        */
        clientChannel = Mecha.getChannels().getOrCreateChannel(id);
        clientChannel.addMember(this);
        addSubscription(id);
    }
    
    /*
     * for Server
    */
    
    public void setAuthorized(boolean authorized) {
        this.authorized = authorized;
    }
    
    public boolean isAuthorized() {
        return authorized;
    }
    
    public ConcurrentHashMap<String, String> getState() {
        return state;
    }
    
    /* 
     * messaging
    */
    
    public Set<String> getSubscriptions() {
        return subscriptions;
    }
    
    public void addSubscription(String channel) {
        subscriptions.add(channel);
    }
    
    public void removeSubscription(String channel) {
        subscriptions.remove(channel);
    }
    
    public PubChannel getChannel() {
        return clientChannel;
    }
    
    /*
     * MVM support
    */
    
    public MVMContext ctx() {
        return ctx;
    }
    
    /*
     * Misc
    */
    
    public String getId() {
        return id;
    }
    
    public WebSocketConnection getConnection() {
        return connection.get();
    }
    
    /*
     * implementation of ChannelConsumer
    */
    
    public void onMessage(String channel, String message) throws Exception {
        JSONObject messageObj = new JSONObject();
        messageObj.put("c", channel);
        messageObj.put("o", message);
        connection.get().send(messageObj.toString());
    }
    
    public void onMessage(String channel, JSONObject message) throws Exception {
        JSONObject messageObj = new JSONObject();
        messageObj.put("c", channel);
        messageObj.put("o", message);
        connection.get().send(messageObj.toString());
    }
    
    /*
     * the byte-based channel requires the sender to
     *  have an implicit understanding with the receivers
    */
    public void onMessage(String channel, byte[] message) throws Exception {
        connection.get().send(message);
    }
}