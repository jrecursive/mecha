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
 * 
 * (C) 2013 John Muellerleile @jrecursive
 *
 */

package mecha.server;

import java.lang.ref.*;
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.zip.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.security.MessageDigest;
import java.net.*;

import mecha.Mecha;
import mecha.server.net.*;
import mecha.util.*;
import mecha.vm.*;
import mecha.vm.parser.*;
import mecha.vm.channels.*;
import mecha.db.MDB;
import mecha.json.*;
import mecha.monitoring.*;
import mecha.json.*;

import org.jboss.netty.channel.ChannelHandlerContext;

public class Server {
    final private static Logger log = 
        Logger.getLogger(Server.class.getName());
    
    final private Rates rates;
    
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
    
    final private AtomicBoolean serverActive;
        
    public Server() throws Exception {
        this.password = Mecha.getConfig().getString("password");
        clientMap = new ConcurrentHashMap<ChannelHandlerContext, Client>();
        clientIdMap = new ConcurrentHashMap<String, Client>();
        
        String addr = Mecha.getConfig().<String>get("server-addr");
        int port = Mecha.getConfig().getInt("server-port");
        netServerThread = new Thread(new MechaServer(addr, port));
        serverActive = new AtomicBoolean(false);
        rates = new Rates();
    }
    
    public void start() throws Exception {
        Mecha.getMonitoring().addMonitoredRates(rates);
        netServerThread.start();
        serverActive.set(true);
        log.info("started");
    }
    
    public void shutdown() throws Exception {
        serverActive.set(false);
        netServerThread.interrupt();
    }
    
    public void onOpen(ChannelHandlerContext connection) {
        try {
            /*
             * If we're just starting up or shutting down, disconnect clients
             *  no matter what.
            */
            if (!serverActive.get()) {
                connection.getChannel().close();
            }

            connectionCount++;
            Client cl = new Client(connection);
            
            /*
             * Reset begins a new "query" and assigns it a cluster-wide
             *  (globally) unique refId.
            */
            cl.getContext().reset();
            
            clientMap.put(connection, cl);
            clientIdMap.put(cl.getId(), cl);
            
            rates.add("mecha.server.connections");
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.server", ex);
            ex.printStackTrace();
        }
    }
    
    public void onClose(ChannelHandlerContext connection) {
        connectionCount--;
        Client cl = clientMap.get(connection);
        if (cl == null) return;
        try {
            cl.getContext().reset();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        clientIdMap.remove(cl.getId());
        if (null != cl) {
            for (String chan: cl.getSubscriptions()) {
                PubChannel pchan = 
                    Mecha.getChannels().getChannel(chan);
                if (pchan != null) {
                    pchan.removeMember(cl);
                    if (pchan.getMembers().size() == 0) {
                        Mecha.getChannels().destroyChannel(pchan.getName());
                        try {
                            Mecha.getChannels().destroyChannel(pchan.getName() + "-c");
                        } catch (Exception ex) {
                            // try to destroy them; -c is the one likely to fail.
                        }
                    }
                }
            }
            try {
                cl.getContext().reset();
            } catch (Exception ex) {
                Mecha.getMonitoring().error("mecha.server", ex);
                ex.printStackTrace();
            }
        }
        clientMap.remove(connection);
        rates.add("mecha.server.disconnections");
    }
    
    public void onMessage(ChannelHandlerContext connection, String request) {
        Client cl = clientMap.get(connection);
        try {
            rates.add("mecha.server.messages-inbound");
            /*
             * If we're just starting up or shutting down, disconnect clients
             *  no matter what.
            */
            
            
            if (!serverActive.get()) {
                connection.getChannel().close();
            }
            
            if (cl == null) {
                /*
                 * This should never happen.
                */
                connection.getChannel().close();
                return;
            }
                        
            /*
             * Command buffering states.
            */
            
            /*
             * Trap lines that end in "{{" and set command
             *  buffering to true.
            */
            if (request.trim().endsWith("{{")) {
                cl.setCommandBuffering(true);
                request = request.trim();
                cl.appendCommandBuffer(
                    request.substring(0,request.length()-2));
                return;
            }
            
            if (cl.isBufferingCommands()) {
                /*
                 * if we end in two closing right braces
                 *  end of block has been signaled and
                 *  we will process the current buffer
                 *  plus the current line (request); if
                 *  we do not end in two closing right braces,
                 *  append to the buffer and return without
                 *  further output.
                */
                if (request.trim().endsWith("}}")) {
                    cl.appendCommandBuffer(
                        request.substring(0,request.length()-2));
                    request = cl.getCommandBuffer();
                    cl.setCommandBuffering(false);
                    cl.clearCommandBuffer();
                } else {
                    cl.appendCommandBuffer(request);
                    return;
                }
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
                    if (cl.withinGlobalBlock()) {
                        Mecha.getMVM().setGlobalBlock(cl.getBlockName(), cl.getBlock());
                    } else {
                        cl.getContext().setBlock(cl.getBlockName(), cl.getBlock());
                    }
                    cl.setWithinBlock(false);
                    cl.setWithinGlobalBlock(false);
                    cl.send(OK_RESPONSE + HashUtils.sha1(cl.getBlockName()));
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
                    cl.send("OK");
                    return;
                } else {
                    log.info("invalid password '" +
                        pass + "'");
                    connection.getChannel().close();
                }
            }
            
            if (!cl.isAuthorized()) {
                connection.getChannel().close();
            }
            
            String response = null;
            
            // used by "warp" (and other rpc mechanisms) to execute
            //  a pre-generated AST
            if (cmd.equals("$execute")) {
                rates.add("mecha.server.warped-execute");
                String astStr = request.substring(cmd.length()+1);
                JSONObject ast = new JSONObject(astStr);
                //log.info("mvm: $execute: " + cl + "/" + cl.getContext() + ": " + ast.toString());
                response = Mecha.getMVM().execute(cl.getContext(), ast);
                        
            /*
             * WarpDelegate "shortcut" to assign a vertex via JSON AST.
            */
            } else if (cmd.equals("$assign")) {
                rates.add("mecha.server.warped-assign");
                String varName = parts[1];
                JSONObject ast = 
                    new JSONObject(request.substring(cmd.length() + varName.length() + 2).trim());
                //log.info("mvm: $assign: " + varName + ": " + cl + "/" + cl.getContext() + ": " + ast.toString());
                Mecha.getMVM().nativeAssignment(cl.getContext(), varName, ast);
                
            /*
             * WarpDelegate "shortcut" to send a JSON control channel message.
            */
            } else if (cmd.equals("$control")) {
                rates.add("mecha.server.warped-control");
                String channel = parts[1];
                JSONObject ast = 
                    new JSONObject(request.substring(cmd.length() + channel.length() + 2).trim());
                //log.info("mvm: $control: " + channel + ": " + cl + "/" + cl.getContext() + ": " + ast.toString());
                Mecha.getMVM().nativeControlMessage(cl.getContext(), channel, ast);
                
            /*
             * WarpDelegate "shortcut" to send a JSON data channel message.
            */
            } else if (cmd.equals("$data")) {
                rates.add("mecha.server.warped-data");
                String channel = parts[1];
                JSONObject ast = 
                    new JSONObject(request.substring(cmd.length() + channel.length() + 2).trim());
                //log.info("mvm: $data: " + channel + ": " + cl + "/" + cl.getContext() + ": " + ast.toString());
                Mecha.getMVM().nativeDataMessage(cl.getContext(), channel, ast);

            /*
             * The only legitimate use for this is to debug
             *  query plans.  When a "done" message is received
             *  (and sent over the wire (see Client)) a reset is
             *  automatically issued to clean up.
            */
            } else if (cmd.equals("$toggle-auto-reset")) {
                cl.toggleAutoReset();

            /*
             * Client-scoped block definition "start" command.
            */
            } else if (cmd.equals("#define-global")) {
                String blockName = parts[1];
                cl.clearBlock();
                cl.setWithinBlock(true);
                cl.setWithinGlobalBlock(true);
                cl.setBlockName(blockName);
                return;
                
            /*
             * Client-scoped block definition "start" command.
            */
            } else if (cmd.equals("#define")) {
                String blockName = parts[1];
                cl.clearBlock();
                cl.setWithinBlock(true);
                cl.setBlockName(blockName);
                return;

            /*
             * Execute a set of MVM commands from a local file.
            */
            } else if (cmd.equals("$exec")) {
                String filename = parts[1];
                response = Mecha.getMVM().execute(cl.getContext(), filename);

            /*
             * disconnect.
            */
            } else if (cmd.equals("$bye") ||
                       cmd.equals("\\q")) {
                connection.getChannel().close();
                return;
            
            } else if (cmd.equals("$interrupt")) {
                for (String clientId : clientIdMap.keySet()) {
                    Client _cl = clientIdMap.get(clientId);
                    log.info(">> " + clientId + ": " + _cl);
                    MVMContext ctx = _cl.getContext();
                    ctx.reset();
                    log.info("--");
                }
                
            /*
             * Return the AST for a given line of instruction(s).
            */
            } else if (cmd.equals("$parse")) {
                String str = request.substring(cmd.length()+1);
                MVMParser mvmParser = new MVMParser();
                JSONObject ast = 
                    mvmParser.parse(str);
                cl.getContext().send(ast);

            // execute mecha vm command
            } else {
                //log.info("mvm: execute: " + cl + "/" + cl.getContext() + ": " + request);
                response = Mecha.getMVM().execute(cl.getContext(), request);
                cl.clearCommandBuffer();
            }
            
            if (response == null) {
                cl.send(OK_RESPONSE + HashUtils.sha1(request));
            } else {
                cl.send(response);
            }
            
        } catch (Exception ex) {
            cl.clearCommandBuffer();
            try {
                JSONObject exceptionMsg = Mecha.getMonitoring().error("mecha.server", ex);
                ex.printStackTrace();
                JSONObject errMsg = new JSONObject();
                errMsg.put("$", "error");
                errMsg.put("error", exceptionMsg);
                cl.send(errMsg.toString());
            } catch (Exception ex1) {
                ex1.printStackTrace();
                // Something is very wrong if we end up here.  A simple
                //  possibility is the client disconnected and we can no
                //  longer send -- that's okay, it will be picked up by
                //  the onClose, etc. methods & cleaned up.  Here we'll
                //  just punt the exception to standard out.
            }
        }
    }
    
    private void send(ChannelHandlerContext connection, String message) throws Exception {
        rates.add("mecha.server.messages-outbound");
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
    
    /*
     * Used by mecha.monitoring.MechaMonitor
    */
    public int getActiveConnectionCount() {
        return connectionCount;
    }
    
    /*
     * Used by mecha.monitoring.MechaMonitor
    */
    public Collection<Client> getClients() {
        return clientIdMap.values();
    }
}

