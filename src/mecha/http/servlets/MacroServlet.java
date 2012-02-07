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
        try {
            log.info(">> " + request);
            log.info(">> " + request.getPathInfo());
            log.info(">> " + request.getQueryString());
            log.info(">> " + request.getServletPath());
            log.info(">> " + request.getParameterMap());
            
            System.out.println("request.getPathInfo = " + request.getPathInfo());
            System.out.println("request.getQueryString = " + request.getQueryString());
            System.out.println("request.getParameterMap = " + request.getParameterMap());
            
            JSONObject params = new JSONObject();
            Map<String, String[]> requestParamMap = request.getParameterMap();
            for(String k : requestParamMap.keySet()) {
                String[] values = requestParamMap.get(k);
                if (values.length == 1) {
                    params.put(k, values[0]);
                } else {
                    params.put(k, values);
                }
            }
            
            String[] parts = request.getPathInfo().substring(1).split("/");
            String namespace = parts[0];
            String macroName = parts[1];
            
            if (parts.length == 1) {
                params.put("$", "#" + macroName);
            } else if (parts.length == 2) {
                params.put("$", "#" + namespace + "." + macroName);
            } else {
                writeError(response, "Invalid request");
                return;
            }
            
            log.info("params: " + params.toString(2));
            //response.setContentType("application/json");
            response.setContentType("text/plain");
            response.setStatus(HttpServletResponse.SC_OK);
            
            
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    private void writeError(HttpServletResponse response, String errorMsg) throws Exception {
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>" + errorMsg + "</h1>");
    }
}

