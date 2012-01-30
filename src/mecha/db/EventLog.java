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
    final private Slab slab;
    
    public EventLog(String name, String path, String mode) throws Exception {
        this.name = name;
        this.path = path;
        slab = new Slab(path, false, mode);
    }
    
    public void append(JSONObject event) throws Exception {
        long offset = slab.append(event.toString().getBytes());
        slab.sync();
        log.info(path + " -> " + offset);
    }
    
    public void close() throws Exception {
        slab.close();
    }
    
}