package mecha.client;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

import net.tootallnate.websocket.Draft;
import net.tootallnate.websocket.WebSocketClient;
import net.tootallnate.websocket.drafts.Draft_76;

import mecha.json.*;

public class MechaClient extends WebSocketClient {
    final private static Logger log = 
        Logger.getLogger(MechaClient.class.getName());

    final private String url;
    final private String password;
    final private MechaClientHandler handler;
    
    private boolean connected = false;
    private boolean waitingForResponse = false;

    public MechaClient(String url, 
                       String password) throws Exception {
        this(url, password, new MechaClientHandler() {
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

            public void onDataMessage(String channel, JSONObject msg) throws Exception {
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
    
    public MechaClient(String url, 
                       String password, 
                       MechaClientHandler handler) throws Exception {
        super(new URI(url), new Draft_76());
        this.url = url;
        this.password = password;
        this.handler = handler;
        
        connect();
        while(!connected) {
            Thread.sleep(1);
        }
        send("auth " + password);
    }
    
    public void onMessage(String message) {
        try {
            // Command-driven :OK <sha1(cmd)> response.
            if (message.startsWith(":OK")) {
                waitingForResponse = false;
                handler.onOk(message.substring(message.indexOf(" ")).trim());
            
            // Potential data message
            } else {
                try {
                    JSONObject msg = new JSONObject(message);
                    
                    // Channel message
                    if (msg.has("c") && msg.has("o")) {
                        String channel = msg.getString("c");
                        JSONObject obj = msg.getJSONObject("o");
                        if (obj.has("$type") &&
                            obj.getString("$type").equals("done")) {
                            waitingForResponse = false;
                            handler.onDoneEvent(channel, obj);
                        } else if (obj.has("$type") &&
                            obj.getString("$type").equals("control")) {
                            handler.onControlEvent(channel, obj);    
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
        waitingForResponse = false;
    }

    public void onClose() {
        connected = false;
        try {
            handler.onClose();
        } catch (Exception ex) {
            handler.onError(ex);
        }
        waitingForResponse = false;
    }

    public void onIOError(IOException ex) {
        handler.onError(ex);
        log.info("onIOError(ex): " + ex.toString());
        ex.printStackTrace();
        waitingForResponse = false;
    }

    public void exec(String cmd) throws Exception {
        exec(cmd, 0);
    }
    
    public void exec(String cmd, int msTimeout) throws Exception {
        long t_st = System.currentTimeMillis();
        while(waitingForResponse) {
            Thread.sleep(10);
            if (msTimeout > 0 &&
                System.currentTimeMillis() - t_st > msTimeout) {
                throw new MechaClient.TimeoutException(cmd + " timed out (" + msTimeout + ")");
            }
        }
        send(cmd);
        waitingForResponse = true;
    }
    
    /*
     * TimeoutException (for exec)
    */
    
    public class TimeoutException extends Exception {
        public TimeoutException(String msg) {
            super(msg);
        }
    }
    
    /*
     * Consumer interface.
    */
    
    public interface MechaClientHandler {
        
        public void onSystemMessage(JSONObject msg) throws Exception;
        
        public void onOpen() throws Exception;
         
        public void onClose() throws Exception;
        
        public void onError(Exception exception);
        
        public void onDoneEvent(String channel, JSONObject msg) throws Exception;
        
        public void onControlEvent(String channel, JSONObject msg) throws Exception;
        
        public void onDataMessage(String channel, JSONObject msg) throws Exception;
        
        public void onOk(String msg) throws Exception;
        
        public void onInfo(String msg) throws Exception;
        
    }
}
