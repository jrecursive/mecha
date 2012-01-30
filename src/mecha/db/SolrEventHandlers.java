package mecha.db;

import java.util.*;
import java.util.logging.*;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.SolrIndexSearcher;

import mecha.Mecha;

public class SolrEventHandlers implements SolrEventListener {
    final private static Logger log = 
        Logger.getLogger(SolrEventHandlers.class.getName());
    
    public void init(NamedList args) { }
    
    public void newSearcher(SolrIndexSearcher newSearcher,
                            SolrIndexSearcher currentSearcher) {
        log.info("newSearcher(" + newSearcher + ", " + currentSearcher + ")");
        /*
         * TODO: statistics
        */
    }
    
    public void postCommit() {
        try {
            log.info("postCommit()");
            Mecha.getEventLogManager().recycle();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
