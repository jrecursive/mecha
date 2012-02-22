package mecha.monitoring;

import java.io.*;
import java.util.*;
import java.util.logging.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.util.*;

public class RiakMonitor {
    final private static Logger log = 
        Logger.getLogger(RiakMonitor.class.getName());
    
    final private Thread riakMonitorThread;
    final private Thread riakLogMonitorThread;
    final private Process riakLogMonitorProcess;
    private String riakURL;
    private String riakStatsURL;
    
    private static String[] explicitStats = {
        "read_repairs_total",
        "read_repairs",
        "coord_redirs_total",
        "sys_process_count",
        "executing_mappers"
    };
    
    public RiakMonitor() throws Exception {
        List<String> riakLogMonitorProcessArgs = 
            new ArrayList<String>();
        String riakBasePath = Mecha.getConfig().<String>get("riak-home");
        if (!riakBasePath.endsWith("/")) {
            riakBasePath += "/";
        }
        riakLogMonitorProcessArgs.add(Mecha.getConfig().<String>get("tail-binary"));
        riakLogMonitorProcessArgs.add("-F");
        for(String logFile : Mecha.getConfig().<List<String>>get("riak-logs")) {
            log.info("monitoring riak_kv log: " + logFile);
            riakLogMonitorProcessArgs.add(riakBasePath + logFile);
        }
        riakLogMonitorProcess = 
            new ProcessBuilder(riakLogMonitorProcessArgs)
                .redirectErrorStream(true)
                .start();
    
        riakMonitorThread = new Thread(new Runnable() {
            public void run() {
                try {
                    while(true) {
                        try {
                            getRiakRuntimeStats();
                            Thread.sleep(1000);
                        } catch (java.lang.InterruptedException iex) {
                            log.info("riak monitor stopped");
                            return;
                        } catch (java.net.ConnectException connectEx) {
                            Mecha.getMonitoring()
                                 .error("mecha.monitoring.riak-monitor.inner", 
                                        connectEx);
                            Mecha.getMonitoring()
                                 .log("mecha.monitoring.riak-monitor.inner", 
                                      "/riak/stats not responding (unable to conect)",
                                      log);
                            Mecha.riakDown();
                            Thread.sleep(30000);
                        } catch (Exception ex) {
                            Mecha.getMonitoring().error("mecha.monitoring.riak-monitor.inner", ex);
                            ex.printStackTrace();
                            Thread.sleep(5000);
                        }
                    }
                } catch (java.lang.InterruptedException iex) {
                    log.info("riak monitor stopped");
                    return;
                } catch (Exception ex) {
                    Mecha.getMonitoring().error("mecha.monitoring.riak-monitor.outer", ex);
                    ex.printStackTrace();
                }
            }
        });

        /*
         * monitor stdout of an instance of 
         *  tail -f <console.log> <error.log> <error.log> [...]
        */
        riakLogMonitorThread = new Thread(new Runnable() {
            public void run() {
                try {
                    InputStream riakLogInputStream = 
                        riakLogMonitorProcess.getInputStream();
                    String logLine;
                    BufferedReader logReader = 
                        new BufferedReader(
                            new InputStreamReader(
                                riakLogMonitorProcess.getInputStream()));
                    while ((logLine = logReader.readLine()) != null) {
                        log.info(logLine.toString());
                        Mecha.getMonitoring()
                             .log("riak.log",
                                  logLine);
                    }
                } catch (Exception ex) {
                    Mecha.getMonitoring().error("mecha.monitoring.riak-monitor.log-monitor.outer", ex);
                    ex.printStackTrace();
                }

            }
        });
    }
    
    protected void stop() throws Exception {
        riakMonitorThread.interrupt();
        riakLogMonitorThread.interrupt();
        riakLogMonitorProcess.destroy();
        log.info("waiting for riak log monitor to stop...");
        riakLogMonitorProcess.waitFor();
        log.info("riak log monitor stopped");
    }
    
    protected void start() throws Exception {
        riakURL = Mecha.getLocalRiakURL();
        riakStatsURL = riakURL + "/stats";

        log.info("* starting riak monitor thread <" + riakURL + ">");
        riakMonitorThread.start();
        riakLogMonitorThread.start();
    }
    
    private void getRiakRuntimeStats() throws Exception {
        JSONObject stats = 
            new JSONObject(
                HTTPUtils.fetch(riakStatsURL));
        
        /*
         * detect & transform all vnode_*, node_*, cpu_*,
         *  pbc_*, & memory_* fields automatically.
        */
        for(String k : JSONObject.getNames(stats)) {
            if (k.startsWith("vnode_") ||
                k.startsWith("node_") ||
                k.startsWith("cpu_") ||
                k.startsWith("pbc_") ||
                k.startsWith("memory_")) {
                String metricName = "riak." + k.replaceAll("_", ".");
                double value = 
                    Double.parseDouble("" + stats.get(k));
                /*
                 * We deal in milliseconds round these parts.
                */
                if (metricName.indexOf(".time") != -1) {
                    value = value / 1000;
                }
                Mecha.getMonitoring().metric(metricName, value);
            }
        }
        
        /*
         * import explicit fields
        */
        for (String k : explicitStats) {
            String metricName = "riak." + k;
            double value = 
                Double.parseDouble("" + stats.get(k));
            /*
             * We deal in milliseconds round these parts.
            */
            if (metricName.indexOf(".time") != -1) {
                value = value / 1000;
            }
            Mecha.getMonitoring().metric(metricName, value);
        }
    }
    
}