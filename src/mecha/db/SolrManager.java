package mecha.db;

import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import mecha.Mecha;

public class SolrManager {
    final private static Logger log = 
        Logger.getLogger(SolrManager.class.getName());
    
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
    
}
