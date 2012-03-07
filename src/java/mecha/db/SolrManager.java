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
import java.util.concurrent.*;
import org.apache.solr.client.solrj.SolrServer;
import mecha.Mecha;

public class SolrManager {
    final private static Logger log = 
        Logger.getLogger(SolrManager.class.getName());
    
    final public static String INDEX_CORE = "index";
    final public static String TMP_CORE = "tmp";
    final public static String SYSTEM_CORE = "system";
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
