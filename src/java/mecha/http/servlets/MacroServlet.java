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
            final JSONObject params = 
                parseParameterMap(request.getParameterMap());
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
                     .getString("server-addr");
            final String password = Mecha.getConfig().getString("password");
            final int port = Mecha.getConfig().getInt("server-port");
            
            final JSONArray dataResult = new JSONArray();
            final Semaphore ready = new Semaphore(1,true);
            final Semaphore done = new Semaphore(1,true);
            
            final boolean oneShotResult;
            if (params.has("_describe") &&
                params.<String>get("_describe").equals("true")) {
                oneShotResult = true;
            } else {
                oneShotResult = false;
            }
            
            ready.acquire();
            done.acquire();
            
            final List remoteExceptions = new ArrayList();
            final List<Exception> localExceptions = new ArrayList<Exception>();
            final MechaClientHandler mechaClientHandler = new MechaClientHandler() {
                public void onSystemMessage(JSONObject msg) throws Exception {
                    log.info("<system> " + msg.toString(2));
                    if (msg.has("$") &&
                        msg.<String>get("$").equals("error")) {
                        onError(new Exception(msg.getJSONObject("error").<String>get("short_message_t")));
                    }
                }
                
                public void onOpen() throws Exception {
                    ready.release();
                }
    
                public void onClose() throws Exception {
                    done.release();
                }
    
                public void onError(Exception ex) {
                    Mecha.getMonitoring()
                         .error("mecha.http.servlets.macro-servlet.remote-exceptions", ex);
                    localExceptions.add(ex);
                    done.release();
                    ex.printStackTrace();
                }
                
                public void onMessage(String msg) {
                    log.info("this should never happen: " + msg);
                }
    
                public void onDataMessage(String channel, JSONObject msg) throws Exception {
                    JSONObject msg1 = new JSONObject();
                    for(String k : JSONObject.getNames(msg)) {
                        if (k.startsWith("$")) continue;
                        msg1.put(k, msg.get(k));
                    }
                    dataResult.put(msg1);
                    if (oneShotResult) {
                        done.release();
                        getTextClient().send("$bye");
                    }
                }
    
                public void onDoneEvent(String channel, JSONObject msg) throws Exception {
                    done.release();
                    getTextClient().send("$bye");
                }
                
                public void onControlEvent(String channel, JSONObject msg) throws Exception {
                    log.info("<control: " + channel + "> " + msg.toString(2));
                }
                
                public void onOk(String msg) throws Exception { }
                
                public void onInfo(String msg) throws Exception {
                    if (msg.equals("OK")) return;
                    log.info("onInfo: " + msg);
                    JSONObject exceptionData = new JSONObject();
                    try {
                        exceptionData = new JSONObject(msg);
                        remoteExceptions.add(exceptionData);
                        done.release();
                    } catch (Exception ex) {
                        exceptionData = new JSONObject();
                        exceptionData.put("error", msg);
                        remoteExceptions.add(exceptionData);
                        done.release();
                    }
                }
            };
            
            MechaClient mechaClient = new MechaClient(host, port, password, mechaClientHandler);
            try {
                ready.acquire();
                if (params.has("_describe") &&
                    params.<String>get("_describe").equals("true")) {
                    dataResult.put(params);
                    mechaClient.exec("$assign _ast " + params.toString());
                    mechaClient.exec("dump-vars");
                } else {
                    mechaClient.exec("$execute " + params.toString());
                }
                
                long t_st = System.currentTimeMillis();
                done.acquire();
                long t_elapsed = System.currentTimeMillis() - t_st;
                if (remoteExceptions.size() == 0) {
                    Mecha.getMonitoring()
                         .metric("mecha.http.macro." + macroName, 
                                 (double) t_elapsed);
                } else {
                    JSONArray exceptions = new JSONArray(remoteExceptions);
                    JSONObject logData = new JSONObject();
                    logData.put("params_obj_s", params);
                    logData.put("exceptions_arr_s", exceptions);
                    Mecha.getMonitoring()
                         .logData("mecha.http.servlets.macro-servlet.remote-exception", 
                                  "macro servlet: remote exceptions", 
                                  logData);
                    writeError(response, logData.toString(2));
                    return;
                }
                if (localExceptions.size() > 0) {
                    for(Exception ex: localExceptions) {
                        throw ex;
                    }
                }
                
                JSONObject result = new JSONObject();
                result.put("elapsed", t_elapsed);
                result.put("result", dataResult);
                response.setContentType("text/plain");
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().println(result.toString(2));
            } finally {
                mechaClient.exec("reset");
                mechaClient.exec("$bye");
                mechaClient = null;
            }
        } catch (Exception ex) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().println(ex.toString());
            ex.printStackTrace();
            Mecha.getMonitoring().error("mecha.http.servlets.macro-servlet", ex);
        }
    }
    
    private void writeError(HttpServletResponse response, String errorMsg) throws Exception {
        response.setContentType("text/plain");
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        response.getWriter().println(errorMsg);
    }
    
    private JSONObject parseParameterMap(Map<String, String[]> requestParamMap) throws Exception {
        JSONObject obj = new JSONObject();
        for(String k : requestParamMap.keySet()) {
            String[] values = requestParamMap.get(k);
            for(int j=0; j<values.length; j++) {
                values[j] = values[j].replaceAll("\"", "\\\"");
            }
            String[] keyParts = k.split("\\.");
            if (keyParts.length == 1) {
                if (values.length == 1) {
                    obj.put(k, values[0]);
                } else {
                    obj.put(k, values);
                }
            } else {
                JSONObject dest = obj;
                String lastPart = keyParts[keyParts.length-1];
                for(int i=0; i<keyParts.length-1; i++) {
                    String keyPart = keyParts[i];
                    if (!dest.has(keyPart)) {
                        dest.put(keyPart, new JSONObject());
                        dest = dest.getJSONObject(keyPart);
                    } else {
                        dest = dest.getJSONObject(keyPart);
                    }
                }
                if (values.length == 1) {
                    dest.put(lastPart, values[0]);
                } else {
                    dest.put(lastPart, values);
                }
            }
        }
        return obj;
    }

}

