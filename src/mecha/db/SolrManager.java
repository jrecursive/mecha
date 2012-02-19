package mecha.db;

import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import org.apache.solr.client.solrj.SolrServer;
import mecha.Mecha;

public class SolrManager {
    final private static Logger log = 
        Logger.getLogger(SolrManager.class.getName());
    
    final private static String INDEX_CORE = "index";
    final private static String TMP_CORE = "tmp";
    final private static String SYSTEM_CORE = "system";
    final private Map<String, SolrCore> cores;
    
    public SolrManager() {
        cores = new ConcurrentHashMap<String, SolrCore>();
    }
    
    public SolrCore getCore(String coreName) throws Exception {
        SolrCore solrInst = cores.get(coreName);
        if (solrInst == null) {
            solrInst = startCore(coreName);
            cores.put(coreName, solrInst);
        }
        return solrInst;
    }
    
    public synchronized SolrCore startCore(String coreName) throws Exception {
        SolrCore solrInst = cores.get(coreName);
        if (solrInst != null) {
            return solrInst;
        }
        String solrHomePath = Mecha.getConfig().getString("solr-home");
        solrInst = 
            new SolrCore(solrHomePath, coreName);
        cores.put(coreName, solrInst);
        return solrInst;
    }
    
    public SolrServer getSolrServer(String coreName) throws Exception {
        return getCore(coreName).getServer();
    }
    
    public SolrServer getIndexServer() throws Exception {
        return getCore(INDEX_CORE).getServer();
    }

    public SolrServer getTmpServer() throws Exception {
        return getCore(TMP_CORE).getServer();
    }

    public SolrServer getSystemServer() throws Exception {
        return getCore(SYSTEM_CORE).getServer();
    }
    
    public void shutdown() throws Exception {
        System.out.println("shutting down " + INDEX_CORE + "...");
        getCore(INDEX_CORE).shutdown();
    }
    
}
