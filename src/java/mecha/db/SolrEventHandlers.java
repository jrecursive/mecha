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
        /*
        Mecha.getMonitoring().log("mecha.db.solr-event-handlers.new-searcher", 
                                  "newSearcher(" + newSearcher + ", " + currentSearcher + ")", log);
        */
        log.info("mecha.db.solr-event-handlers.new-searcher: " + 
                 "newSearcher(" + newSearcher + ", " + currentSearcher + ")");
    }
    
    public void postCommit() {
        try {
            try {
                Mecha.getMDB().commit();
            } catch (Exception ex) {
                ex.printStackTrace();
                log.info("MDB commit failed! Rolling back index core commit!");
                // TODO: solr rollback
                throw ex;
            }
            
            Mecha.lastCommit = System.currentTimeMillis() / 1000;
            
            Mecha.getMonitoring().log("mecha.db.solr-event-handlers.post-commit", 
                                      "solr post commit hook", log);
            Mecha.getEventLogManager().recycle();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public void postSoftCommit() {
        try {
            log.info("postSoftCommit()");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
