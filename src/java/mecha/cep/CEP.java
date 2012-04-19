package mecha.cep;

import java.util.*; 
import java.util.logging.*;
import java.util.concurrent.*;

import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.analysis.miscellaneous.PatternAnalyzer;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;

import mecha.Mecha;
import mecha.json.*;

public class CEP {
    final private static Logger log = 
        Logger.getLogger(CEP.class.getName());

    /*
     * e.g.,
     *  "put" -> [ CEQuery, ... ],
     *  "delete" -> [ CEQuery, ... ]
     * 
    */
    final private ConcurrentHashMap<String, List<CEQuery>>
        queryMap;
    final private LinkedBlockingQueue<CEvent> eventQueue;
    final private Thread processorThread;
    
    final private Runnable processor = new Runnable() {
        public void run() {
            PatternAnalyzer analyzer = PatternAnalyzer.DEFAULT_ANALYZER;
            // todo: pre-parse? reuse?
            StandardQueryParser parser = new StandardQueryParser(analyzer);
            parser.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);
            
            List<CEQuery> deadQueries = new ArrayList<CEQuery>();
            
            while(true) {
                try {
                    CEvent e = eventQueue.take();
                    JSONObject obj = e.getObj();
                    MemoryIndex idx = new MemoryIndex();
                    for(String f : JSONObject.getNames(obj)) {
                        String val;
                        if (f.endsWith("s_mv")) {
                            val = "";
                            List ar = obj.get(f);
                            for(Object o : ar) {
                                val += " " + o;
                            }
                            val = val.trim();
                        } else {
                            val = "" + obj.get(f);
                        }
                        // todo: possibly different analyzer per field type
                        idx.addField(f, val, analyzer);
                    }
                    List<CEQuery> queries = queryMap.get(e.getType());
                    for(CEQuery q : queries) {
                        if (!Mecha.getChannels().channelExists(q.getOutputChannel())) {
                            log.info("dead query channel for: " + q.getQuery());
                            deadQueries.add(q);
                            continue;
                        }
                        if (idx.search(parser.parse(q.getQuery(), "_"))>0.0f) {
                            log.info("match! " + q.getQuery());
                            try {
                                Mecha.getChannels().send(q.getOutputChannel(), obj);
                            } catch (Exception deadChannelEx) {
                                log.info("dead query channel for: " + q.getQuery());
                                deadQueries.add(q);
                            }
                        } // else, no match
                    }
                    if (deadQueries.size()>0) {
                        for(CEQuery qry : deadQueries) {
                            queries.remove(qry);
                        }
                        deadQueries.clear();
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    };
    
    public CEP() {
        log.info("* initializing cep engine");
        queryMap = new ConcurrentHashMap<String, List<CEQuery>>();
        eventQueue = new LinkedBlockingQueue<CEvent>();
        processorThread = new Thread(processor);
        log.info("* starting processor thread");
        processorThread.start();
    }
    
    public void process(String type, JSONObject obj) {
        try {
            if (!queryMap.containsKey(type)) return;
            CEvent e = new CEvent(type, obj);
            eventQueue.add(e);
        } catch (Exception ex) {
            // todo: monitoring, logging
            ex.printStackTrace();
        }
    }
    
    public void addQuery(String type, String queryStr, String outputChannel) throws Exception {
        List<CEQuery> queries;
        if (!queryMap.contains(type)) {
            queries = new CopyOnWriteArrayList<CEQuery>();
            queryMap.put(type, queries);
        } else {
            queries = queryMap.get(type);
        }
        CEQuery q = new CEQuery(queryStr, outputChannel);
        queries.add(q);
        queryMap.put(type, queries);
        log.info("addQuery(" + type + ", " + queryStr + ", " + outputChannel + ") :: " + queryMap);
    }

}