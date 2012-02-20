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
        Mecha.getMonitoring().log("mecha.db.solr-event-handlers.new-searcher", 
                                  "newSearcher(" + newSearcher + ", " + currentSearcher + ")", log);
    }
    
    public void postCommit() {
        try {
            Mecha.getMonitoring().log("mecha.db.solr-event-handlers.post-commit", 
                                      "solr post commit hook", log);
            Mecha.getEventLogManager().recycle();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
