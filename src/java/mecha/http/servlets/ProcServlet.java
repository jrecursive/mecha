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
 */

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

import org.apache.commons.lang.*;

import mecha.Mecha;
import mecha.vm.*;
import mecha.client.*;
import mecha.json.*;
import mecha.monitoring.*;
import mecha.util.*;

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
             * /proc/metrics
            */
            if (subsystem.equals("metrics")) {
                result = doMetricsRequest(parts, params);
            
            /*
             * /proc/top
            */
            } else if (subsystem.equals("top")) {
                result = doTopRequest(parts, params);
              
            /*
             * /proc/node
            */  
            } else if (subsystem.equals("node")) {
                result = doNodeRequest(parts, params);
            
            /* 
             * /proc/config
            */
            } else if (subsystem.equals("config")) {
                result = doConfigRequest(parts, params);
                
            
            /*
             * /proc/cluster
            */
            } else if (subsystem.equals("cluster")) {
                result = doClusterRequest(parts, params);
            
            /*
             * /proc/riak
            */
            } else if (subsystem.equals("riak")) {
                result = doRiakRequest(parts, params);
            
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
     * /proc/metrics          list of all metric names
     * /proc/metrics/all      most recent value of all metrics
     * /proc/metrics/<name>   most recent value of <name>
    */
    private JSONObject doMetricsRequest(String[] parts, JSONObject params) throws Exception {
        int entries;
        boolean summary = false;
        if (params.has("entries")) {
            entries = Integer.parseInt("" + params.get("entries"));
        } else {
            entries = 30;
        }
        if (params.has("all") &&
            params.<String>get("all").equals("true")) {
            return doClusterMetricsRequest(params, entries);
        }
        if (params.has("summary") &&
            params.<String>get("summary").equals("true")) {
            summary = true;
        }
        return Mecha.getMonitoring().asJSON(entries, summary);
    }
    
    private JSONObject doClusterMetricsRequest(JSONObject params, int entries) throws Exception {
        boolean summary = false;
        if (params.has("summary") &&
            params.<String>get("summary").equals("true")) {
            summary = true;
        }
        Set<String> hosts = Mecha.getRiakRPC().getClusterHosts();
        JSONObject result = new JSONObject();
        for(String host : hosts) {
            String url = "http://" + host + ":" + 
                Mecha.getConfig().get("http-port") +
                "/proc/metrics?entries=" + entries + 
                "&summary=" + summary;
            try {
                JSONObject obj = 
                    new JSONObject(HTTPUtils.fetch(url));
                result.put(host, obj.getJSONObject("result"));
            } catch(Exception ex) {
                ex.printStackTrace();
            }
        }
        return result;
    }
    
    /*
     * /proc/top
    */
    private JSONObject doTopRequest(String[] parts, JSONObject params) throws Exception {
        JSONObject result = new JSONObject();
        return result;
    }
    
    /*
     * /proc/node
    */
    private JSONObject doNodeRequest(String[] parts, JSONObject params) throws Exception {
        JSONObject result = new JSONObject();
        
        /*
         * last-commit
        */
        if (parts.length > 1 &&
            parts[1].equals("last-commit")) {
            result.put("last-commit", Mecha.lastCommit);
        }
        
        /*
         * do, e.g.,
         *
         * http://localhost:7283/proc/node/do/127.0.0.1/?u=/proc/config
         *  or
         * http://localhost:7283/proc/node/do/?host=127.0.0.1&u=/proc/config
         *
         * TODO: I don't like these requests, they all need consistency; this 
         *  is turning into a junk drawer.
         *
        */
        if (parts.length > 1 &&
            parts[1].equals("do")) {
            String host;
            if (parts.length > 2) {
                host = parts[2];
            } else {
                host = params.<String>get("host");
            }
            String urlFragment = params.<String>get("u");
            if (!urlFragment.startsWith("/")) {
                urlFragment = "/" + urlFragment;
            }
            result = doNode(host, urlFragment);
        }
        
        return result;
    }
    
    /*
     * /proc/node
    */
    private JSONObject doRiakRequest(String[] parts, JSONObject params) throws Exception {
        JSONObject result = new JSONObject();
        
        /*
         * do, e.g.,
         *
         * http://localhost:7283/proc/node/do/127.0.0.1/?u=/proc/config
         *  or
         * http://localhost:7283/proc/node/do/?host=127.0.0.1&u=/proc/config
         *
         * TODO: I don't like these requests, they all need consistency; this 
         *  is turning into a junk drawer.
         *
        */
        if (parts.length > 1 &&
            parts[1].equals("do")) {
            String urlFragment = params.<String>get("u");
            if (!urlFragment.startsWith("/")) {
                urlFragment = "/" + urlFragment;
            }
            result = doRiak(urlFragment);
        }
        
        return result;
    }
    
    /*
     * a "do this everywhere & consolidate replies" utility
    */
    private JSONObject doClusterRequest(String[] parts, JSONObject params) throws Exception {
        JSONObject result = new JSONObject();
        
        /*
         * do, e.g.,
         *
         * http://localhost:7283/proc/cluster/do?u=/proc/config
         *
        */
        if (parts.length > 1 &&
            parts[1].equals("do")) {
            String urlFragment = params.<String>get("u"); // StringEscapeUtils.unescapeHtml(
            if (!urlFragment.startsWith("/")) {
                urlFragment = "/" + urlFragment;
            }
            result = doCluster(urlFragment);
        }
        return result;
    }

    private JSONObject doCluster(String urlFragment) throws Exception {
        Set<String> hosts = Mecha.getRiakRPC().getClusterHosts();
        JSONObject result = new JSONObject();
        for(String host : hosts) {
            String url = "http://" + host + ":" + 
                Mecha.getConfig().get("http-port") +
                urlFragment;
            try {
                JSONObject obj = 
                    new JSONObject(HTTPUtils.fetch(url));
                result.put(host, obj);
            } catch(Exception ex) {
                result.put(host, "Error: " + ex.toString());
                ex.printStackTrace();
            }
        }
        return result;
    }
    
    private JSONObject doNode(String host, String urlFragment) throws Exception {
        JSONObject result = new JSONObject();
        Set<String> hosts = Mecha.getRiakRPC().getClusterHosts();
        if (!hosts.contains(host)) {
            result.put("error", "no such host");
            return result;
        }
        String url = "http://" + host + ":" + 
            Mecha.getConfig().get("http-port") +
                urlFragment;
        try {
            JSONObject obj = 
                new JSONObject(HTTPUtils.fetch(url));
            result.put(host, obj);
        } catch(Exception ex) {
            result.put("exception", ex.toString());
            ex.printStackTrace();
        }
        return result;
    }
    
    private JSONObject doRiak(String urlFragment) throws Exception {
        JSONObject result = new JSONObject();
        String url = "http://" + Mecha.getConfig().get("server-addr") + ":" + 
            Mecha.getConfig().get("riak-http-port") +
                urlFragment;
        try {
            String response = HTTPUtils.fetch(url);
            if (response.startsWith("{")) {
                result.put("response", new JSONObject(response));
            } else if (response.startsWith("[")) {
                result.put("response", new JSONArray(response));
            } else {
                result.put("response", response);
            }
        } catch(Exception ex) {
            result.put("exception", ex.toString());
            ex.printStackTrace();
        }
        return result;
    }
    
    /*
     * /proc/proc/<ctx-ref>           MVMContext dump-vars
     * /proc/proc/<ctx-ref>/<ref-id>  MVMFunction state & metrics
    */
    private JSONObject doConfigRequest(String[] parts, JSONObject params) throws Exception {
        return Mecha.getConfig();
    }
    
}


