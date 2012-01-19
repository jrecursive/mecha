package mecha.server;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;

import org.webbitserver.*;
import org.webbitserver.handler.*;
import org.webbitserver.handler.exceptions.*;

import mecha.vm.*;

/*
 * client record
*/
public class Client {
    /*
     * communication
    */
    public boolean authorized = false;
    public ConcurrentHashMap<String, String> state;
    
    /*
     * messaging
    */
    public Set<String> subscriptions;
    public WeakReference<WebSocketConnection> connection;
    
    /*
     * MVM
    */
    public MVMContext ctx;
    
    public Client(WebSocketConnection connection) {
        state = new ConcurrentHashMap<String, String>();
        this.connection = new WeakReference<WebSocketConnection>(connection);
        subscriptions = Collections.synchronizedSet(new HashSet());
        
        /*
         * MVMContext keeps WeakReference<Client>
        */
        ctx = new MVMContext(this);
    }
    
    
    
}