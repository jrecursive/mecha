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
import mecha.db.MDB;

import org.webbitserver.*;
import org.webbitserver.handler.*;
import org.webbitserver.handler.exceptions.*;

import org.json.*;

public class Server implements WebSocketHandler {
    final private static Logger log = 
        Logger.getLogger(Server.class.getName());

    private String password;
    private int connectionCount;
    private WebServer webServer;
    
    /*
     * map: names -> channels
    */
    static Map<String, PubChannel> channelMap;
    
    /*
     * map: connections -> clients
    */
    static Map<WebSocketConnection, Client> clientMap;
    
    static {
        channelMap = new ConcurrentHashMap<String, PubChannel>();
        clientMap = new ConcurrentHashMap<WebSocketConnection, Client>();
    }
    
    public Server() throws Exception {
        this.password = Mecha.getConfig().getString("password");
    }
    
    public void start() throws Exception {
        webServer = 
            WebServers.createWebServer(Mecha.getConfig().getInt("client-port"))
                .add(Mecha.getConfig().getString("client-endpoint"), this)
                .add(new StaticFileHandler(Mecha.getConfig().getString("client-www-root")))
                .start();
        log.info("started");
    }        
    
    /**
     * Sends a message to all subscribers of specified channel
     *
     * @param chan  the channel to send msg to
     * @param msg   the message to send to all subscribers of chan
     *
     */
    private void send(String chan, String msg) throws Exception {
        PubChannel pchan = channelMap.get(chan);
        if (pchan == null) {
            log.info("ERR :no such channel");
        } else {
            int recip = 0;
            JSONObject obmsg = new JSONObject();
            obmsg.put("channel", chan);
            obmsg.put("msg", new JSONObject(msg));
            for(Client client: pchan.members) {
                WebSocketConnection clientConnection = client.connection.get();
                if (clientConnection != null) {
                    clientConnection.send(obmsg.toString());
                    recip++;
                }
            }
            log.info("send(" + chan + ", " + msg + "): " + recip + " recipients");
        }
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
            log.info("client cleanup");
            for (String chan: cl.subscriptions) {
                PubChannel pchan = channelMap.get(chan);
                pchan.members.remove(cl);
                log.info("removed subscription to " + chan + " for " + cl + " <" + connection + ">");
            }
        }
        clientMap.remove(connection);
        log.info("WEBSOCKETS: DISCONNECTED: " + connection);
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
                    cl.authorized = true;
                    connection.send("OK");
                    return;
                } else {
                    connection.close();
                }
            }
            
            if (!cl.authorized) {
                connection.close();
            }
            
            // subscribe <chan>
            if (cmd.startsWith("sub")) {
                String chan = parts[1];
                log.info(connection + ": sub: " + chan);
                PubChannel pchan = channelMap.get(chan);
                if (pchan == null) {
                    pchan = new PubChannel(chan);
                    channelMap.put(chan, pchan);
                }
                pchan.members.add(cl);
                cl.subscriptions.add(chan);
                connection.send("OK");
                
            // unsubscribe <chan>
            } else if (cmd.startsWith("unsub")) {
                String chan = parts[1];
                log.info(connection + ": unsub: " + chan);
                PubChannel pchan = channelMap.get(chan);
                if (pchan == null) {
                    connection.send("ERR :no such channel");
                } else {
                    pchan.members.remove(cl);
                    cl.subscriptions.remove(chan);
                    connection.send("OK");
                }
                
            // publish <chan> <msg>
            } else if (cmd.startsWith("pub")) {
                String chan = parts[1];
                PubChannel pchan = channelMap.get(chan);
                if (pchan == null) {
                    connection.send("ERR :no such channel");
                } else {
                    String msg = request.substring(request.indexOf(parts[1])+parts[1].length()).trim();
                    for(Client client: pchan.members) {
                        if (client != cl) {
                            WebSocketConnection clientConnection = client.connection.get();
                            if (clientConnection != null) {
                                send(chan, msg);
                            }
                        }
                    }
                    connection.send("OK");
                }
               
            // unhandled
            } else {
                log.info("mvm: execute: " + cl + "/" + cl.ctx + ": " + request);
                MVM mvm = new MVM();
                connection.send(mvm.execute(cl.ctx, request));
                
                //log.info("Unknown request: " + request);
                //connection.send("ERR :unknown request :" + request + "");
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

