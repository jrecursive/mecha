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
public class ProcServlet extends HttpServlet {
    final private static Logger log = 
        Logger.getLogger(ProcServlet.class.getName());

    public ProcServlet() {
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
            
            String subsystem = parts[0];
            
            JSONObject result;
            
            long t_start = System.currentTimeMillis();
            
            /*
             * /system/metrics
            */
            if (subsystem.equals("metrics")) {
                result = doMetricsRequest(parts, params);
            
            /*
             * /system/top
            */
            } else if (subsystem.equals("top")) {
                result = doTopRequest(parts, params);
              
            /*
             * /system/cluster
            */  
            } else if (subsystem.equals("cluster")) {
                result = doClusterRequest(parts, params);
            
            /* 
             * /system/proc
            */
            } else if (subsystem.equals("proc")) {
                result = doProcRequest(parts, params);
            
            /*
             * unknown request
            */
            } else {
                response.getWriter().println("bad request\n");
                return;
            }
            long t_elapsed = System.currentTimeMillis() - t_start;
            
            JSONObject resultObj = new JSONObject();
            resultObj.put("elapsed", t_elapsed);
            resultObj.put("result", result);
            response.getWriter().println(resultObj.toString(2));
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
    
    /*
     * /system/metrics          list of all metric names
     * /system/metrics/all      most recent value of all metrics
     * /system/metrics/<name>   most recent value of <name>
    */
    private JSONObject doMetricsRequest(String[] parts, JSONObject params) throws Exception {
        JSONObject result = new JSONObject();
        return result;
    }
    
    /*
     * /system/top
    */
    private JSONObject doTopRequest(String[] parts, JSONObject params) throws Exception {
        JSONObject result = new JSONObject();
        return result;
    }
    
    /*
     * /system/cluster
    */
    private JSONObject doClusterRequest(String[] parts, JSONObject params) throws Exception {
        JSONObject result = new JSONObject();
        return result;
    }
    
    /*
     * /system/proc/<ctx-ref>           MVMContext dump-vars
     * /system/proc/<ctx-ref>/<ref-id>  MVMFunction state & metrics
    */
    private JSONObject doProcRequest(String[] parts, JSONObject params) throws Exception {
        JSONObject result = new JSONObject();
        return result;
    }
    
}
