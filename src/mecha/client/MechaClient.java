package mecha.client;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

import net.tootallnate.websocket.Draft;
import net.tootallnate.websocket.WebSocketClient;
import net.tootallnate.websocket.drafts.Draft_10;
import net.tootallnate.websocket.drafts.Draft_17;
import net.tootallnate.websocket.drafts.Draft_75;
import net.tootallnate.websocket.drafts.Draft_76;

public class MechaClient extends WebSocketClient {
    final private static Logger log = 
        Logger.getLogger(MechaClient.class.getName());

    final private String url;
    final private String password;
    
    private boolean connected = false;

    public MechaClient(String url, String password) throws Exception {
        super(new URI(url), new Draft_76());
        this.url = url;
        this.password = password;
        connect();
        
        while(!connected) {
            Thread.sleep(1);
        }
        send("auth " + password);
    }
    
    public void onMessage(String message) {
        log.info(message);
    }
    
    public void onOpen() {
        connected = true;
        log.info("onOpen()");
    }

    public void onClose() {
        connected = false;
        log.info("onClose()");
    }

    public void onIOError(IOException ex) {
        log.info("onIOError(ex): " + ex.toString());
        ex.printStackTrace();
    }

    public void exec(String cmd) throws IOException {
        send(cmd);
    }
}
