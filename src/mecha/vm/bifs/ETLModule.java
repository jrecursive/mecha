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
     * Sequence a vector of results into a stream of 
     *  ordered data elements according to a simple
     *  alpha ordering of a specified field.
     *
     * See UDVectorSequencer for user-defined ordering
     *  predicates (using blocks & specified jx.l.vm).
     *
    */
    public class VectorSequencer extends MVMFunction {
        final String field;
        final boolean isAscending;
        final String dataField;
        final Comparator comparatorFun;
        final Collator collator;
        
        public VectorSequencer(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            field = config.getString("field");
            if (config.getString("order").startsWith("asc")) {
                isAscending = true;
            } else {
                isAscending = false;
            }
            if (config.has("data-field")) {
                dataField = config.getString("data-field");
            } else {
                dataField = "data";
            }
            collator = Collator.getInstance();
            comparatorFun = new Comparator<Object>() {
                public int compare(Object obj1, Object obj2) {
                    try {
                        String value1 = (String)((LinkedHashMap)obj1).get(field);
                        String value2 = (String)((LinkedHashMap)obj2).get(field);
                        if (isAscending) {
                            return collator.compare(value1, value2);
                        } else {
                            return collator.compare(value2, value1);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    return 0;
                }
            };
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            List<Object> msgVec = msg.getJSONArray(dataField).asList();
            Collections.<Object>sort(msgVec, comparatorFun);
            for(Object sortedMsg : msgVec) {
                broadcastDataMessage(new JSONObject((LinkedHashMap)sortedMsg));
            }
        }
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