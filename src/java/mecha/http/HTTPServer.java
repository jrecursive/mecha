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

package mecha.http;

import java.net.*;
import java.util.logging.*;
import javax.servlet.http.HttpServlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import mecha.Mecha;
import mecha.http.servlets.*;
import mecha.http.websockets.*;
import mecha.json.*;

public class HTTPServer {
    final private static Logger log = 
        Logger.getLogger(HTTPServer.class.getName());

    final private int port;
    final private String addr;
    final private String wwwRoot;
    
    final private Server server;
    final private ServletContextHandler context;
    
    final private WebSocketServer webSocketServer;

    public HTTPServer() throws Exception {
        port = Mecha.getConfig().getInt("http-port");
        addr = Mecha.getConfig().getString("server-addr");
        wwwRoot = Mecha.getConfig().getString("www-root");
        
        //server = new Server(new InetSocketAddress(addr, port));
        // xxx todo: tmp
        server = new Server(port);
        context = new ServletContextHandler(ServletContextHandler.NO_SECURITY);
        context.setContextPath("/");
        server.setHandler(context);
        
        // Serve admin content.
        ServletHolder holder = 
            context.addServlet(org.eclipse.jetty.servlet.DefaultServlet.class, "/admin/*");
        holder.setInitParameter("resourceBase", wwwRoot);
        holder.setInitParameter("pathInfoOnly", "true");
        
        context.addServlet(new ServletHolder(new MacroServlet()),"/mecha/*");
        context.addServlet(new ServletHolder(new ProcServlet()),"/proc/*");
        context.addServlet(new ServletHolder(new ProxyServlet()),"/proxy/*");
        
        log.info("* starting websocket server");
        webSocketServer = new WebSocketServer();
    }
    
    public void start() throws Exception {
        server.start();
        webSocketServer.start();
    }
    
}