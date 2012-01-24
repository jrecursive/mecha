package mecha.server;

import java.lang.ref.*;
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.zip.*;
import java.util.concurrent.*;
import java.security.MessageDigest;
import java.net.*;

import mecha.Mecha;
import mecha.util.*;
import mecha.vm.*;
import mecha.vm.channels.*;
import mecha.db.MDB;

import org.webbitserver.*;
import org.webbitserver.handler.*;
import org.webbitserver.handler.exceptions.*;

import org.json.*;

public class Server implements WebSocketHandler {
    final private static Logger log = 
        Logger.getLogger(Server.class.getName());
    
    final private String password;
    final private WebServer webServer;
    
    private int connectionCount;
        
    /*
     * map: connections -> clients
    */
    final private Map<WebSocketConnection, Client> clientMap;
        
    public Server() throws Exception {
        this.password = Mecha.getConfig().getString("password");
        clientMap = new ConcurrentHashMap<WebSocketConnection, Client>();
        webServer = 
            WebServers.createWebServer(Mecha.getConfig().getInt("client-port"))
                .add(Mecha.getConfig().getString("client-endpoint"), this)
                .add(new StaticFileHandler(Mecha.getConfig().getString("client-www-root")));
    }
    
    public void start() throws Exception {
        webServer.start();
        log.info("started");
    }        
    
    public void onOpen(WebSocketConnection connection) {
        connectionCount++;
        Client cl = new Client(connection);
        clientMap.put(connection, cl);
    }
    
    public void onClose(WebSocketConnection connection) {
        connectionCount--;
        Client cl = clientMap.get(connection);
        if (null != cl) {
            log.info("* client cleanup: " + connection);
            for (String chan: cl.getSubscriptions()) {
                PubChannel pchan = 
                    Mecha.getChannels().getChannel(chan);
                if (pchan != null) {
                    pchan.removeMember(cl);
                }
                log.info("removed subscription to " + chan + " for " + cl + " <" + connection + ">");
            }
        }
        clientMap.remove(connection);
        log.info("disconnect: " + connection);
    }
    
    public void onMessage(WebSocketConnection connection, String request) {
        try {
            Client cl = clientMap.get(connection);
            
            if (cl == null) {
                connection.send("ERR :no client for connection :this should never happen");
                return;
            }
            
            // command processing here
            String[] parts = request.split(" ");
            String cmd = parts[0];
            
            // auth <password>
            if (cmd.startsWith("auth")) {
                String pass = parts[1];
                if (password.equals(pass)) {
                    cl.setAuthorized(true);
                    connection.send("OK");
                    return;
                } else {
                    connection.close();
                }
            }
            
            if (!cl.isAuthorized()) {
                connection.close();
            }
            
            // subscribe <chan>
            if (cmd.equals("/sub")) {
                String chan = parts[1];
                log.info(connection + ": sub: " + chan);
                
                PubChannel pchan = 
                    Mecha.getChannels().getOrCreateChannel(chan);
                pchan.addMember(cl);
                cl.addSubscription(chan);
                connection.send("OK");
                
            // unsubscribe <chan>
            } else if (cmd.equals("/unsub")) {
                String chan = parts[1];
                log.info(connection + ": unsub: " + chan);
                PubChannel pchan = 
                    Mecha.getChannels().getOrCreateChannel(chan);
                if (pchan == null) {
                    connection.send("ERR :no such channel");
                } else {
                    pchan.members.remove(cl);
                    cl.removeSubscription(chan);
                    connection.send("OK");
                }
                
            // publish <chan> <msg>
            } else if (cmd.equals("!")) {
                String chan = parts[1];
                PubChannel pchan = 
                    Mecha.getChannels().getChannel(chan);
                if (pchan == null) {
                    connection.send("ERR :no such channel");
                } else {
                    String msg = request.substring(request.indexOf(parts[1])+parts[1].length()).trim();
                    pchan.send(msg);
                    connection.send("OK");
                }
               
            // execute mecha vm command
            } else {
                log.info("mvm: execute: " + cl + "/" + cl.ctx() + ": " + request);
                MVM mvm = new MVM();
                connection.send(mvm.execute(cl.ctx(), request));
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public void onMessage(WebSocketConnection connection, byte[] message) {
        log.info("onMessage(" + connection + ", <" + message.length + " bytes> " + (new String(message)) + ")");
    }
    
    public void onPong(WebSocketConnection connection, String message) {
        log.info("onPong(" + connection + ", " + message + ")");
    }
}

