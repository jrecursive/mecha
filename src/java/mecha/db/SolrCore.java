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

import java.io.*;
import java.util.*;
import java.util.logging.*;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.servlet.SolrRequestParsers;

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
    
    public void shutdown() throws Exception {
        System.out.println("forcing shutdown commit .. ");
        //server.commit(true, false);
        System.out.println(coreName + ": shutdown ok.");
    }
}
