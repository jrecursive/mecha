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
                ex.printStackTrace();
                log.info(msg.toString(2));
            }
        }
    }


}