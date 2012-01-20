package mecha.db;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

public class SolrCore {
    final private static Logger log = 
        Logger.getLogger(SolrCore.class.getName());

    final private String homePath;
    final private String coreName;
    final private EmbeddedSolrServer server;
    final private CoreContainer container;

    public SolrCore(String solrHomePath,
                    String solrCoreName) throws Exception {
        homePath = solrHomePath;
        coreName = solrCoreName;
        log.info("starting solr core [" + homePath + "] " + coreName);
        File f = new File(new File(solrHomePath), "solr.xml" );
        container = new CoreContainer();
        container.load(solrHomePath, f );
        server = new EmbeddedSolrServer(container, solrCoreName);
    }
    
    public String getHomePath() {
        return homePath;
    }
    
    public String getCoreName() {
        return coreName;
    }
    
    public SolrServer getServer() {
        return server;
    }
    
    public CoreContainer getCoreContainer() {
        return container;
    }
}
