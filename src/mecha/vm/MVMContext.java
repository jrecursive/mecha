package mecha.vm;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;

import mecha.server.*;

public class MVMContext {    
    final private ConcurrentHashMap<String, Object> vars;
    final private WeakReference<Client> clientRef;
    
    public MVMContext(Client client) {
        clientRef = new WeakReference<Client>(client);
        vars = new ConcurrentHashMap<String, Object>();
    }
    
    /*
     * MVM context-scope variables
    */
    
    public ConcurrentHashMap<String, Object> getVars() {
        return vars;
    }
    
    public <T> T get(String key) {
        return (T)vars.get(key);
    }
        
    public void put(String key, Object val) {
        vars.put(key, val);
    }
    
    public void remove(String key) {
        vars.remove(key);
    }
    
    public void clearVars() {
        vars.clear();
    }
    
    /*
     * helpers
    */
    
    public Client getClient() {
        return clientRef.get();
    }
    
    public String getClientId() {
        return clientRef.get().getId();
    }
}