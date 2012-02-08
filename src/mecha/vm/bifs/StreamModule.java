package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;
import java.text.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.*;
import mecha.vm.channels.*;
import mecha.vm.flows.*;

public class StreamModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(StreamModule.class.getName());

    public StreamModule() throws Exception {
        super();
    }

    public void moduleLoad() throws Exception { }
    
    public void moduleUnload() throws Exception { }
    
    /*
     * Eat "eats" a specified number of data messages 
     *  before it starts passing them through.
    */
    public class Eat extends MVMFunction {
        final int total;
        int eaten;
        boolean full;
        
        public Eat(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            total = Integer.parseInt(config.getString("count"));
            eaten = 0;
            full = false;
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            eaten++;
            if (eaten <= total) {
                log.info("bitbucketed " + eaten + " of " + total);
                return;
            }
            log.info("passing through " + msg);
            broadcastDataMessage(msg);
        }
    }

    /*
     * Limit allows a specified number of data messages to pass
     *  through it.
    */
    public class Limit extends MVMFunction {
        final int total;
        int count;
        
        public Limit(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            total = Integer.parseInt(config.getString("count"));
            count = 0;
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            if (count > total) {
                return;
            } else if (count < total) {
                broadcastDataMessage(msg);
            } else if (count == total) {
                broadcastDone();
            }
            count++;
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            /*
             * If all upstream sources have completed and a "done" 
             *  message has filtered down to this and we are not
             *  yet to the specified count to limit, pass
             *  the message through, else swallow it (since we
             *  will never meet the criteria in onDataMessage if
             *  there is no more data coming and we have not reached
             *  the maximum count).
            */
            if (count <= total) {
                broadcastDone(msg);
            }
        }
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

}