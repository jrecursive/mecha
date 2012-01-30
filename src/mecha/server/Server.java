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

import mecha.json.*;

public class Server implements WebSocketHandler {
    final private static Logger log = 
        Logger.getLogger(Server.class.getName());
    
    final private static String OK_RESPONSE = ":OK ";
    
    final private String password;
    final private WebServer webServer;
    private int connectionCount;
        
    /*
     * map: connections -> clients
     * map: client ids -> clients
    */
    final private Map<WebSocketConnection, Client> clientMap;
    final private Map<String, Client> clientIdMap;
        
    public Server() throws Exception {
        this.password = Mecha.getConfig().getString("password");
        clientMap = new ConcurrentHashMap<WebSocketConnection, Client>();
        clientIdMap = new ConcurrentHashMap<String, Client>();
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
        try {
            connectionCount++;
            Client cl = new Client(connection);
            
            /*
             * Reset begins a new "query" and assigns it a cluster-wide
             *  (globally) unique refId.
            */
            cl.getContext().reset();
            
            clientMap.put(connection, cl);
            clientIdMap.put(cl.getId(), cl);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public void onClose(WebSocketConnection connection) {
        connectionCount--;
        Client cl = clientMap.get(connection);
        clientIdMap.remove(cl.getId());
        if (null != cl) {
            log.info("* client cleanup: " + connection);
            for (String chan: cl.getSubscriptions()) {
                PubChannel pchan = 
                    Mecha.getChannels().getChannel(chan);
                if (pchan != null) {
                    pchan.removeMember(cl);
                }
                log.info("removed subscription to " + chan + 
                    " for " + cl + " <" + connection + ">");
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
                    pchan.removeMember(cl);
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
                log.info("mvm: execute: " + cl + "/" + cl.getContext() + ": " + request);
                String response = Mecha.getMVM().execute(cl.getContext(), request);
                if (response == null) {
                    connection.send(OK_RESPONSE + HashUtils.sha1(request));
                } else {
                    connection.send(response);
                }
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
    
    /*
     * helpers
    */
    
    public Client getClient(String clientId) {
        return clientIdMap.get(clientId);
    }
}

