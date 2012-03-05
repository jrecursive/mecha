package mecha.db;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.io.*;

import mecha.json.*;

public class EventLog {
    final private static Logger log = 
        Logger.getLogger(EventLog.class.getName());
    
    final private String name;
    final private String path;
    final private String mode;
    private Slab slab;
    
    public EventLog(String name, String path, String mode) throws Exception {
        this.name = name;
        this.path = path;
        this.mode = mode;
        slab = new Slab(path, false, mode);
    }
    
    public void append(JSONObject event) throws Exception {
        long offset = slab.append(event.toString().getBytes());
        log.info(path + " -> " + offset);
    }
    
    public void close() throws Exception {
        slab.close();
    }
    
    public String getName() {
        return name;
    }
    
    public String getFilename() {
        return path;
    }
    
    public void recycle() throws Exception {
        log.info("recycle: close: " + path);
        slab.close();
        slab = null;
        
        log.info("recycle: unlink: " + path);
        File file = new File(path);
        file.delete();
        
        log.info("reopen: " + path);
        slab = new Slab(path, false, mode);
        
        log.info("recycle complete <" + path + ">");
    }   
}