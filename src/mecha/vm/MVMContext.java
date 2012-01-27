package mecha.vm;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;

import mecha.Mecha;
import mecha.server.*;
import mecha.json.*;
import mecha.vm.*;
import mecha.vm.flows.*;

public class MVMContext {    
    final private ConcurrentHashMap<String, Object> vars;
    final private WeakReference<Client> clientRef;
    
    private Flow flow;
    private String refId;

    public MVMContext(Client client) throws Exception {
        clientRef = new WeakReference<Client>(client);
        vars = new ConcurrentHashMap<String, Object>();
        flow = new Flow();
        refId = Mecha.guid(MVMContext.class);
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
     * flow
    */
    
    public void clearFlow() {
        flow = new Flow();
    }
    
    public Flow getFlow() {
        return flow;
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
    
    public String getRefId() {
        return refId;
    }
    
    /*
     * Context-specific messaging
    */
    
    public void send(JSONObject msg) throws Exception {
        getClient().getChannel().send(msg);
    }
    
    /*
     * Clear all assignments (vars) and create a new empty flow.
    */
    
    public void reset() {
        clearVars();
        clearFlow();
    }
    
}