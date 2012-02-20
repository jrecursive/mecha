package mecha.http.servlets;

import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.io.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import mecha.Mecha;
import mecha.vm.*;
import mecha.client.*;
import mecha.json.*;
import mecha.monitoring.*;

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
            response.setCharacterEncoding("UTF-8");
            
            log.info(">> " + request);
            log.info(">> " + request.getPathInfo());
            log.info(">> " + request.getQueryString());
            log.info(">> " + request.getServletPath());
            log.info(">> " + request.getParameterMap());
            
            System.out.println("request.getPathInfo = " + request.getPathInfo());
            System.out.println("request.getQueryString = " + request.getQueryString());
            System.out.println("request.getParameterMap = " + request.getParameterMap());
            
            final JSONObject params = new JSONObject();
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
            String macroName;
            if (parts.length == 1) {
                macroName = parts[0];
            } else if (parts.length == 2) {
                macroName = parts[0] + "." + parts[1];
            } else {
                writeError(response, "Invalid request");
                return;
            }
            params.put("$", "#" + macroName);
            
            final String host = 
                Mecha.getConfig()
                     .getJSONObject("riak-config")
                     .getString("pb-ip");
            final String password = Mecha.getConfig().getString("password");
            final int port = Mecha.getConfig().getInt("client-port");
            
            log.info("params: " + params.toString(2));
            
            final JSONArray dataResult = new JSONArray();
            final Semaphore ready = new Semaphore(1,true);
            final Semaphore done = new Semaphore(1,true);
            ready.acquire();
            done.acquire();
            final MechaClientHandler mechaClientHandler = new MechaClientHandler() {
                public void onSystemMessage(JSONObject msg) throws Exception {
                    log.info("<system> " + msg.toString(2));
                }
                
                public void onOpen() throws Exception {
                    //log.info("<connected>");
                    ready.release();
                }
    
                public void onClose() throws Exception {
                    //log.info("<disconnected>");
                    done.release();
                }
    
                public void onError(Exception ex) {
                    //log.info("<error> " + ex.toString());
                    done.release();
                    ex.printStackTrace();
                }
                
                public void onMessage(String msg) {
                    log.info("this should never happen: " + msg);
                }
    
                public void onDataMessage(String channel, JSONObject msg) throws Exception {
                    //log.info("<data: " + channel + "> " + msg.toString(2));
                    JSONObject msg1 = new JSONObject();
                    for(String k : JSONObject.getNames(msg)) {
                        if (k.startsWith("$")) continue;
                        msg1.put(k, msg.get(k));
                    }
                    dataResult.put(msg1);
                }
    
                public void onDoneEvent(String channel, JSONObject msg) throws Exception {
                    log.info("<done: " + channel + "> " + msg.toString(2));
                    done.release();
                    getTextClient().send("$bye");
                }
                
                public void onControlEvent(String channel, JSONObject msg) throws Exception {
                    log.info("<control: " + channel + "> " + msg.toString(2));
                }
                
                public void onOk(String msg) throws Exception {
                    //log.info("<ok> " + msg);
                }
                
                public void onInfo(String msg) throws Exception {
                    log.info("<info> " + msg);
                }
            };
            
            MechaClient mechaClient = new MechaClient(host, port, password, mechaClientHandler);
            ready.acquire();
            mechaClient.exec("$execute " + params.toString());
            long t_st = System.currentTimeMillis();
            done.acquire();
            long t_elapsed = System.currentTimeMillis() - t_st;
            
            Mecha.getMonitoring()
                 .metric("mecha.http.macro." + macroName, 
                         (double) t_elapsed);
            
            response.setContentType("text/plain");
            response.setStatus(HttpServletResponse.SC_OK);
            JSONObject result = new JSONObject();
            result.put("elapsed", t_elapsed);
            result.put("result", dataResult);
            response.getWriter().println(result.toString(2));
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.http.servlets.macro-servlet", ex);
            ex.printStackTrace();
        }
    }
    
    private void writeError(HttpServletResponse response, String errorMsg) throws Exception {
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>" + errorMsg + "</h1>");
    }
}

