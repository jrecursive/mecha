/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
    
    public EventLog createEventLogForWrite(String eventLogName, 
                                                        String eventLogFilename) throws Exception {
        log.info("\n\ncreateEventLogForWrite\n\n" + eventLogName + " -> " + eventLogFilename);
        EventLog eventLog = eventLogs.get(eventLogName);
        if (eventLog == null) {
            log.info(eventLogFilename);
            File f = new File(eventLogFilename);
            if (f.exists()) {
                throw new Exception(eventLogFilename + " already exists!");
            }
            eventLog = new EventLog(eventLogName, eventLogFilename, getEventLogMode());
            log.info(eventLogName + ": " + eventLogFilename + " opened");
            eventLogs.put(eventLogName, eventLog);
        }
        return eventLog;
    }
    
    public EventLog getEventLog(String eventLogName) throws Exception {
        return eventLogs.get(eventLogName);
    }
    
    public static String getEventLogBasePath() throws Exception {
        return Mecha.getConfig().getString("event-log-directory");
    }

    public static String getEventLogMode() throws Exception {
        return Mecha.getConfig().getString("event-log-mode");
    }
    
    public void shutdown() throws Exception {
        for(String eventLogName : eventLogs.keySet()) {
            EventLog eventLog = eventLogs.get(eventLogName);
            log.info("shutdown: " + eventLogName + ": " + eventLog.getFilename() + ": close");
            eventLog.close();
        }
        eventLogs.clear();
        log.info("event logs shutdown");
    }
    
    public void recycle() throws Exception {
        log.info("recycling event logs...");
        for(String eventLogName : eventLogs.keySet()) {
            EventLog eventLog = eventLogs.get(eventLogName);
            log.info("shutdown: " + eventLogName + ": " + eventLog.getFilename() + ": recycle");
            eventLog.recycle();
        }
        log.info("event logs recycled");
    }
}
