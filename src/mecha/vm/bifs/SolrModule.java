package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrDocument;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.*;

public class SolrModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(SolrModule.class.getName());
    
    public SolrModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
        log.info("moduleLoad()");   
    }
    
    public void moduleUnload() throws Exception {
        log.info("moduleUnload()");
    }
    
    public class Select extends MVMFunction {
        public Select(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
        
        public void onStartEvent(JSONObject startEventMsg) throws Exception {
            JSONObject selectParams = getConfig().getJSONObject("params");
            long t_st = System.currentTimeMillis();
            long start = 0;
            long batchSize = 1000;
            long count = 0;
            long rowLimit = -1;
            long rawFound = 0;
            
            /*
             * Rewrite scored queries into filter queries.
            */
            if (selectParams.has("q") &&
                !selectParams.has("fq")) {
                selectParams.put("fq", selectParams.get("q"));
                selectParams.put("q", "*:*");
            }
            
            if (selectParams.has("start")) {
                start = Long.parseLong("" + selectParams.get("start"));
                selectParams.remove("start");
            }
            
            if (selectParams.has("rows")) {
                rowLimit = Long.parseLong("" + selectParams.get("rows"));
                selectParams.remove("rows");
            }
            
            while(true) {
                ModifiableSolrParams solrParams = new ModifiableSolrParams();
                for(String k : JSONObject.getNames(selectParams)) {
                    solrParams.set(k, "" + selectParams.get(k));
                }
                solrParams.set("start", "" + start);
                solrParams.set("rows", "" + batchSize);
                
                int batchCount = 0;
                QueryResponse res = 
                    Mecha.getSolrManager().getIndexServer().query(solrParams);
                rawFound = res.getResults().getNumFound();
                if (res.getResults().getNumFound() == 0) break;
                if (rowLimit == -1) {
                    rowLimit = res.getResults().getNumFound();
                }
                for(SolrDocument doc : res.getResults()) {
                    JSONObject msg = new JSONObject();
                    for(String fieldName : doc.getFieldNames()) {
                        msg.put(fieldName, doc.get(fieldName));
                    }
                    broadcastDataMessage(msg);
                    count++; 
                    batchCount++;
                    if (count >= rowLimit) break;
                }
                //log.info("batchCount: " + batchCount + " start: " + start + " count: " + count + " numFound: " + 
                //    res.getResults().getNumFound());
                start += batchCount;
                if (start >= rowLimit) break;
            }
            long t_elapsed = System.currentTimeMillis() - t_st;
            JSONObject doneMsg = new JSONObject();
            doneMsg.put("elapsed", t_elapsed);
            doneMsg.put("count", count);
            doneMsg.put("found", rawFound);
            broadcastDone(doneMsg);
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            log.info("onCancelEvent: " + msg.toString(2));
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            log.info("onDoneEvent: " + msg.toString(2));
        }
    }
}