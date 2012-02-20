package mecha.monitoring;

import java.util.*;
import java.util.logging.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.util.*;

public class RiakMonitor {
    final private static Logger log = 
        Logger.getLogger(RiakMonitor.class.getName());
    
    final private Thread riakMonitorThread;
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
        riakMonitorThread = new Thread(new Runnable() {
            public void run() {
                try {
                    while(true) {
                        try {
                            getRiakRuntimeStats();
                            Thread.sleep(10000);
                        } catch (Exception ex) {
                            Mecha.getMonitoring().error("mecha.monitoring.riak-monitor.inner", ex);
                            ex.printStackTrace();
                            Thread.sleep(5000);
                        }
                    }
                } catch (Exception ex) {
                    Mecha.getMonitoring().error("mecha.monitoring.riak-monitor.outer", ex);
                    ex.printStackTrace();
                }
            }
        });
    }
    
    protected void start() throws Exception {
        riakURL = Mecha.getLocalRiakURL();
        riakStatsURL = riakURL + "/stats";

        log.info("* starting riak monitor thread <" + riakURL + ">");
        riakMonitorThread.start();
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