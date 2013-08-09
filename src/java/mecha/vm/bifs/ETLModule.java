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
 * 
 * (C) 2013 John Muellerleile @jrecursive
 *
 */

package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;
import java.text.Collator;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.response.FacetField;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.*;

public class ETLModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(ETLModule.class.getName());
    
    public ETLModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
        log.info("moduleLoad()");   
    }
    
    public void moduleUnload() throws Exception {
        log.info("moduleUnload()");
    }
        
    /*
     * Project a subset of fields.
    */
    public class Project extends MVMFunction {
        final Set<String> fields;
        
        public Project(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            fields = new HashSet<String>();
            if (config.get("fields") instanceof String) {
                fields.add(config.getString("fields"));
            } else {
                JSONArray fieldArray = config.getJSONArray("fields");
                for (int i=0; i<fieldArray.length(); i++) {
                    final String field = fieldArray.getString(i);
                    fields.add(field);
                }
            }
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            JSONObject newObj = new JSONObject();
            for(String field : fields) {
                if (msg.has(field)) {
                    newObj.put(field, msg.get(field));
                }
            }
            broadcastDataMessage(newObj);
        }
    }
    
    /*
     * Extract & reconstitute the JSONObject within a Riak object.
    */
    public class ExtractRiakValue extends MVMFunction {
        final int position;
        
        public ExtractRiakValue(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            position = Integer.parseInt(config.getString("position"));
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            try {
                broadcastDataMessage(new JSONObject(
                    msg.getJSONArray("values")
                       .getJSONObject(position)
                       .getString("data")));
            } catch (Exception ex) {
                Mecha.getMonitoring().error("mecha.vm.bifs.etl-module", ex);
                ex.printStackTrace();
                log.info(msg.toString(2));
            }
        }
    }


}