package mecha.monitoring;

import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;

import mecha.Mecha;
import mecha.vm.MVMContext;
import mecha.client.*;
import mecha.server.*;
import mecha.json.*;
import mecha.util.*;

public class MechaMonitor {
    final private static Logger log = 
        Logger.getLogger(MechaMonitor.class.getName());
    
    final private Thread mechaMonitorThread;
    
    public MechaMonitor() throws Exception {
        mechaMonitorThread = new Thread(new Runnable() {
            public void run() {
                try {
                    while(true) {
                        try {
                            getMechaRuntimeStats();
                            Thread.sleep(1000);
                        } catch (Exception ex) {
                            Mecha.getMonitoring().error("mecha.monitoring.mecha-monitor.inner", ex);
                            ex.printStackTrace();
                            Thread.sleep(10000);
                        }
                    }
                } catch (Exception ex) {
                    Mecha.getMonitoring().error("mecha.monitoring.mecha-monitor.outer", ex);
                    ex.printStackTrace();
                }
            }
        });
    }
    
    protected void start() throws Exception {
        log.info("* starting mecha monitor thread");
        mechaMonitorThread.start();
    }
    
    private void getMechaRuntimeStats() throws Exception {
        /*
         * This periodically collects statistics 
         *  from mecha subsystems.
        */
        
        Monitoring m = Mecha.getMonitoring();
        
        /*
         * Server
        */
        m.metric("mecha.server.active-connections", 
            Mecha.getServer().getActiveConnectionCount());
        
        int numVars = 0;
        int numFuns = 0;
        int numBlocks = 0;
        int numFibers = 0;
        int numMemChans = 0;
        
        int threadActive = 0;
        int threadComplete = 0;
        int threadPoolSize = 0;
        int threadQueueDepth = 0;
        long threadTaskCount = 0;
        
        for(Client client : Mecha.getServer().getClients()) {
            MVMContext ctx = client.getContext();
            numVars += ctx.getNumVars();
            numFuns += ctx.getNumFuns();
            numBlocks += ctx.getNumBlocks();
            numFibers += ctx.getNumFibers();
            numMemChans += ctx.getNumMemoryChannels();
            
            ThreadPoolExecutor funEx = 
                (ThreadPoolExecutor) ctx.getFunctionExecutor();
            threadActive += funEx.getActiveCount();
            threadComplete += funEx.getCompletedTaskCount();
            threadPoolSize += funEx.getPoolSize();
            threadQueueDepth += funEx.getQueue().size();
            threadTaskCount += funEx.getTaskCount();
        }
        
        m.metric("mecha.mvm.global.vars", numVars);
        m.metric("mecha.mvm.global.functions", numFuns);
        m.metric("mecha.mvm.global.blocks", numBlocks);
        m.metric("mecha.mvm.global.fibers", numFibers);
        m.metric("mecha.mvm.global.memory-channels", numMemChans);
        m.metric("mecha.mvm.global.functions.active", threadActive);
        m.metric("mecha.mvm.global.functions.complete", threadComplete);
        m.metric("mecha.mvm.global.functions.pool-size", threadPoolSize);
        m.metric("mecha.mvm.global.functions.queue-depth", threadQueueDepth);
        m.metric("mecha.mvm.global.functions.task-count", threadTaskCount);
        
        /*
         * Channels
        */
        m.metric("mecha.vm.channels.active",
                 Mecha.getChannels().getChannelNames().size());
                 
        
    }
    
}