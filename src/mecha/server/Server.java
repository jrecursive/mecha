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
import mecha.server.net.*;
import mecha.util.*;
import mecha.vm.*;
import mecha.vm.channels.*;
import mecha.db.MDB;
import mecha.json.*;

import org.jboss.netty.channel.ChannelHandlerContext;
import mecha.json.*;

public class Server {
    final private static Logger log = 
        Logger.getLogger(Server.class.getName());
    
    final private static String OK_RESPONSE = ":OK ";
    
    final private Thread netServerThread;
    
    final private String password;
    private int connectionCount;
        
    /*
     * map: connections -> clients
     * map: client ids -> clients
    */
    final private Map<ChannelHandlerContext, Client> clientMap;
    final private Map<String, Client> clientIdMap;
        
    public Server() throws Exception {
        this.password = Mecha.getConfig().getString("password");
        clientMap = new ConcurrentHashMap<ChannelHandlerContext, Client>();
        clientIdMap = new ConcurrentHashMap<String, Client>();
        
        int port = Mecha.getConfig().getInt("client-port");
        netServerThread = new Thread(new MechaServer(port));
    }
    
    public void start() throws Exception {
        netServerThread.start();
        log.info("started");
    }
    
    public void onOpen(ChannelHandlerContext connection) {
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
    
    public void onClose(ChannelHandlerContext connection) {
        connectionCount--;
        Client cl = clientMap.get(connection);
        if (cl == null) return;
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
            try {
                log.info("resetting context");
                cl.getContext().reset();
                log.info("cleanup complete <" + cl.toString() + ">");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        clientMap.remove(connection);
        log.info("disconnect: " + connection);
    }
    
    public void onMessage(ChannelHandlerContext connection, String request) {
        try {
            Client cl = clientMap.get(connection);
            
            if (cl == null) {
                send(connection, "ERR :no client for connection :this should never happen");
                return;
            }
            
            /*
             * Append to current block if in
             *  block mode (via $define-block)
            */
            if (cl.withinBlock()) {
                /*
                 * End of block.
                */
                if (request.equals("#end " + cl.getBlockName())) {
                    cl.setWithinBlock(false);
                    cl.getContext().setBlock(cl.getBlockName(), cl.getBlock());
                    send(connection, OK_RESPONSE + HashUtils.sha1(cl.getBlockName()));
                    cl.setBlockName(null);
                    cl.clearBlock();
                } else {
                    cl.appendBlock(request);
                }
                
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
                    send(connection, "OK");
                    return;
                } else {
                    connection.getChannel().close();
                }
            }
            
            if (!cl.isAuthorized()) {
                connection.getChannel().close();
            }
            
            String response = null;
            
            // used by "warp" (and other rpc mechanisms) to execute
            //  a pre-generated AST
            if (cmd.equals("$exec")) {
                String astStr = cmd.substring(cmd.length()+1);
                JSONObject ast = new JSONObject(astStr);
                log.info("mvm: execute: " + cl + "/" + cl.getContext() + ": " + ast.toString());
                response = Mecha.getMVM().execute(cl.getContext(), ast);

            /*
             * Block definition commands.
            */
            } else if (cmd.equals("#define")) {
                String blockName = parts[1];
                log.info("blockName = " + blockName);
                cl.clearBlock();
                cl.setWithinBlock(true);
                cl.setBlockName(blockName);
                return;

            // execute mecha vm command
            } else {
                log.info("mvm: execute: " + cl + "/" + cl.getContext() + ": " + request);
                response = Mecha.getMVM().execute(cl.getContext(), request);
            }
            
            if (response == null) {
                send(connection, OK_RESPONSE + HashUtils.sha1(request));
            } else {
                send(connection, response);
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    private void send(ChannelHandlerContext connection, String message) throws Exception {
        connection.getChannel().write(message + "\n");
    }
    
    public void onMessage(ChannelHandlerContext connection, byte[] message) {
        log.info("onMessage(" + connection + ", <" + message.length + " bytes> " + (new String(message)) + ")");
    }
    
    /*
     * helpers
    */
    
    public Client getClient(String clientId) {
        return clientIdMap.get(clientId);
    }
}

