package mecha.db;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;

import mecha.Mecha;
import mecha.util.*;
import mecha.json.*;

public class EventLogManager {
    final private static Logger log = 
        Logger.getLogger(EventLogManager.class.getName());
    
    final private Map<String, EventLog> eventLogs;
    
    public EventLogManager() {
        eventLogs = new ConcurrentHashMap<String, EventLog>();
    }
    
    public synchronized EventLog createEventLogForWrite(String eventLogName) throws Exception {
        EventLog eventLog = eventLogs.get(eventLogName);
        if (eventLog == null) {
            String eventLogFilename = 
                getEventLogBasePath() + "/" + 
                HashUtils.sha1(eventLogName) + "." + 
                System.currentTimeMillis() + "." + 
                "eventlog";
            log.info(eventLogFilename);
            File f = new File(eventLogFilename);
            if (f.exists()) {
                throw new Exception(eventLogFilename + " already exists!");
            }
            eventLog = new EventLog(eventLogName, eventLogFilename, getEventLogMode());
            log.info(eventLogFilename + " opened");
            eventLogs.put(eventLogName, eventLog);
        } else {
            throw new Exception ("eventLog already open!");
        }
        return eventLog;
    }
    
    public EventLog getEventLog(String eventLogName) throws Exception {
        return eventLogs.get(eventLogName);
    }
    
    private String getEventLogBasePath() throws Exception {
        return Mecha.getConfig().getString("event-log-directory");
    }

    private String getEventLogMode() throws Exception {
        return Mecha.getConfig().getString("event-log-mode");
    }
    
    public synchronized void shutdown() throws Exception {
        for(String eventLogName : eventLogs.keySet()) {
            EventLog eventLog = eventLogs.get(eventLogName);
            eventLog.close();
        }
        eventLogs.clear();
    }
}
