package mecha.server;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.webbitserver.*;
import org.webbitserver.handler.*;
import org.webbitserver.handler.exceptions.*;

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
    
    /*
     * Mecha VM
    */
    final private MVMContext ctx;
    
    /*
     * per-client identifiers
    */
    final private String id;
    
    public Client(WebSocketConnection connection) {
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
    
    public void onMessage(String message) throws Exception {
        JSONObject messageObj = new JSONObject();
        messageObj.put("channel", channel);
        messageObj.put("msg", message);
        connection.get().send(messageObj);
    }
    
    public void onMessage(JSONObject message) throws Exception {
        JSONObject messageObj = new JSONObject();
        messageObj.put("channel", channel);
        messageObj.put("msg", message);
        connection.get().send(messageObj);
    }
    
    /*
     * the byte-based channel requires the sender to
     *  have an implicit understanding with the receivers
    */
    public void onMessage(byte[] message) throws Exception {
        connection.get().send(message);
    }
}