package mecha;

import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;

import org.json.*;

import mecha.util.*;
import mecha.server.*;
import mecha.db.*;
import mecha.jinterface.*;

public class Mecha {
    final private static Logger log = 
        Logger.getLogger(Mecha.class.getName());

    final private static JSONObject config
        = Mecha.loadConfig(TextFile.get("config.json"));
    
    final private MDB mdb;
    final private Server server;
    final private SolrManager solrManager;
    final private RiakConnector riakConnector;
    final private RiakRPC riakRPC;
    
    /*
     * startup & init
    */
    
    private static Mecha mechaInst;
    static {
        try {
            mechaInst = new Mecha();
            mechaInst.start();
        } catch (Exception ex) {
            ex.printStackTrace();
            log.info("unable to start mecha");
            System.exit(-1);
        }
    }
    
    public static void main(String args[]) 
        throws Exception { }
    
    private Mecha() throws Exception {
        log.info("* establishing riak link");
        riakRPC = new RiakRPC();
        JSONObject riakConfig = config.getJSONObject("riak-config");
        riakConfig.put("local-url", riakRPC.getLocalRiakURL());
        riakConfig.put("pb-ip", riakRPC.getLocalPBIP());
        riakConfig.put("pb-port", riakRPC.getLocalPBPort());
        log.info("# introspected riak configuration: " + riakConfig.toString(2));
    
        log.info("* starting solr cores");
        solrManager = new SolrManager();
        
        log.info("* starting mdb");
        mdb = new MDB();
        
        log.info("* starting query server");
        server = new Server();
        
        log.info("* starting web services");
        // TODO: jetty ws integ
        
        log.info("* starting riak connector");
        riakConnector = new RiakConnector(mdb);
    }
    
    public void start() throws Exception {
        startSolrCores();
        mdb.startMDB();
        riakConnector.startConnector();
        server.start();
    }
    
    /* 
     * getters
    */
    
    public static Mecha get() {
        return mechaInst;
    }
    
    public static JSONObject getConfig() {
        return config;
    }
    
    public static MDB getMDB() {
        return get().mdb;
    }
    
    public static Server getServer() {
        return get().server;
    }
    
    public static SolrManager getSolrManager() {
        return get().solrManager;
    }
    
    public static RiakConnector getRiakConnector() {
        return get().riakConnector;
    }
    
    /*
     * helpers
    */
    
    private static void startSolrCores() throws Exception {
        System.setProperty("solr.data.dir", getConfig().getString("solr-data-dir"));
        System.setProperty("solr.tmp.data.dir", getConfig().getString("solr-temporary-data-dir"));
        JSONArray cores = getConfig().getJSONArray("solr-cores");
        for(int i=0; i<cores.length(); i++) {
            String coreName = cores.getString(i);
            log.info("/ starting core: " + coreName);
            if (null == get().solrManager.startCore(coreName)) {
                throw new Exception ("Cannot start solr core '" + coreName);
            }
        }
    }
    
    private static JSONObject loadConfig(String configFileStr) {
        try {
            StringBuffer cfsb = new StringBuffer();
            String[] configFileStrLines = configFileStr.split("\n");
            for(String configFileLine : configFileStrLines) {
                String cleanLine;
                if (configFileLine.indexOf("##")!=-1) {
                    cleanLine = configFileLine.substring(0,configFileLine.indexOf("##"));
                } else {
                    cleanLine = configFileLine;
                }
                int idx;
                while((idx = cleanLine.indexOf("${"))!=-1) {
                    int idx1 = cleanLine.indexOf("}", idx);
                    String line0 = cleanLine.substring(0,idx);
                    String line1 = cleanLine.substring(idx1+1);
                    String varName = cleanLine.substring(idx+2, idx1).trim();
                    cleanLine = line0 + System.getenv(varName) + line1;
                }
                if (cleanLine.trim().equals("")) continue;
                cfsb.append(cleanLine);
                cfsb.append("\n");
            }
            try {
                return new JSONObject(cfsb.toString());
            } catch (Exception configException) {
                configException.printStackTrace();
                log.info("could not parse config file!  processed version: ");
                log.info(cfsb.toString());
                throw configException;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
        }
        return null;
    }
}
