package mecha.http.servlets;

import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import java.io.*;

import mecha.Mecha;
import mecha.vm.*;
import mecha.client.*;
import mecha.json.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class MacroServlet extends HttpServlet {
    final private static Logger log = 
        Logger.getLogger(MacroServlet.class.getName());

    public MacroServlet() {
        log.info("<constructor>");
    }
    
    protected void doGet(HttpServletRequest request, 
                         HttpServletResponse response) 
        throws ServletException, IOException {
        
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>Hi From MacroServlet</h1>");
        
        
        
    }
}

