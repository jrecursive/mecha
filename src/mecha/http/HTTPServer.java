package mecha.http;

import java.util.logging.*;
import javax.servlet.http.HttpServlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import mecha.Mecha;
import mecha.http.servlets.*;
import mecha.json.*;

public class HTTPServer {
    final private static Logger log = 
        Logger.getLogger(HTTPServer.class.getName());

    final private int port;
    final private String wwwRoot;
    
    final private Server server;
    final private ServletContextHandler context;

    public HTTPServer() throws Exception {
        port = Mecha.getConfig().getInt("http-port");
        wwwRoot = Mecha.getConfig().getString("www-root");
        
        server = new Server(port);
        context = new ServletContextHandler(ServletContextHandler.NO_SECURITY);
        context.setContextPath("/");
        server.setHandler(context);
        
        // Serve admin content.
        ServletHolder holder = 
            context.addServlet(org.eclipse.jetty.servlet.DefaultServlet.class, "/www/*");
        holder.setInitParameter("resourceBase", wwwRoot);
        holder.setInitParameter("pathInfoOnly", "true");
        
        context.addServlet(new ServletHolder(new MacroServlet()),"/mecha/*");
    }
    
    public void start() throws Exception {
        server.start();
    }
    
}