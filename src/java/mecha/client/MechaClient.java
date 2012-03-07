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

package mecha.client;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.atomic.AtomicBoolean;

import mecha.json.*;
import mecha.client.net.*;

public class MechaClient extends MechaClientHandler {
    final private static Logger log = 
        Logger.getLogger(MechaClient.class.getName());
    
    final private String host;
    final private int port;
    final private String password;
    final private MechaClientHandler handler;
    final private TextClient textClient;
    
    private boolean connected = false;

    public MechaClient(String host, 
                       int port,
                       String password) throws Exception {
        this(host, port, password, new MechaClientHandler() {
            public void onSystemMessage(JSONObject msg) throws Exception {
                log.info("<system> " + msg.toString(2));
            }
            
            public void onOpen() throws Exception {
                log.info("<connected>");
            }

            public void onClose() throws Exception {
                log.info("<disconnected>");
            }

            public void onError(Exception ex) {
                log.info("<error> " + ex.toString());
                ex.printStackTrace();
            }
            
            public void onMessage(String msg) {
                log.info("this should never happen: " + msg);
            }

            public void onDataMessage(String channel, JSONObject msg1) throws Exception {
                JSONObject msg = new JSONObject();
                for(String f : JSONObject.getNames(msg1)) {
                    if (f.startsWith("$")) continue;
                    msg.put(f, msg1.get(f));
                }
                log.info("<data: " + channel + "> " + msg.toString(2));
            }

            public void onDoneEvent(String channel, JSONObject msg) throws Exception {
                log.info("<done: " + channel + "> " + msg.toString(2));
            }
            
            public void onControlEvent(String channel, JSONObject msg) throws Exception {
                log.info("<control: " + channel + "> " + msg.toString(2));
            }
            
            public void onOk(String msg) throws Exception {
                log.info("<ok> " + msg);
            }
            
            public void onInfo(String msg) throws Exception {
                log.info("<info> " + msg);
            }
        });
    }
    
    public MechaClient(String host, 
                       int port, 
                       String password, 
                       MechaClientHandler handler) throws Exception {
        this.host = host;
        this.port = port;
        this.password = password;
        this.handler = handler;
        textClient = new TextClient(host, port, password, this);
        handler.setTextClient(textClient);
    }
    
    public void onMessage(String message) {
        try {
            // Command-driven :OK <sha1(cmd)> response.
            if (message.startsWith(":OK")) {
                handler.onOk(message.substring(message.indexOf(" ")).trim());
            
            // Potential data message
            } else {
                try {
                    JSONObject msg = new JSONObject(message);
                    
                    // Channel message
                    if (msg.has("c") && msg.has("o")) {
                        String channel = msg.getString("c");
                        JSONObject obj = msg.getJSONObject("o");
                        if (obj.has("$") &&
                            obj.getString("$").equals("done")) {
                            handler.onDoneEvent(channel, obj);
                        } else if (obj.has("$") &&
                            obj.getString("$").equals("control")) {
                            handler.onControlEvent(channel, obj);    
                        } else if (obj.has("$") &&
                            obj.getString("$").equals("cancel")) {
                            handler.onDoneEvent(channel, obj);    
                        } else {
                            handler.onDataMessage(channel, obj);
                        }
                        
                    // System message (no specified origin)
                    } else {
                        handler.onSystemMessage(msg);
                    }
                    
                // System info message (not a json object)
                } catch (Exception ex0) {
                    handler.onInfo(message);
                }
            }
        
        // General error.  Trap & send error event.
        } catch (Exception ex) {
            handler.onError(ex);
        }
    }
    
    public void onOpen() {
        connected = true;
        try {
            handler.onOpen();
        } catch (Exception ex) {
            handler.onError(ex);
        }
    }

    public void onClose() {
        connected = false;
        try {
            handler.onClose();
        } catch (Exception ex) {
            handler.onError(ex);
        }
    }

    public void onError(Exception ex) {
        handler.onError(ex);
        log.info("onError(ex): " + ex.toString());
        ex.printStackTrace();
    }

    public void exec(String cmd) throws Exception {
        textClient.send(cmd);
    }
    
}
