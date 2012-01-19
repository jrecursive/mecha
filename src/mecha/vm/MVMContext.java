package mecha.vm;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;

import mecha.server.*;

public class MVMContext {    
    private ConcurrentHashMap<String, Object> vars;
    private WeakReference<Client> clientRef;
    
    public MVMContext(Client client) {
        this.clientRef = new WeakReference<Client>(client);
        vars = new ConcurrentHashMap<String, Object>();
    }
    
    public ConcurrentHashMap<String, Object> getVars() {
        return vars;
    }
    
    public <T> T get(String key) {
        return (T)vars.get(key);
    }
    
    public void put(String key, Object val) {
        vars.put(key, val);
    }
    
    public Client getClient() {
        return clientRef.get();
    }
}