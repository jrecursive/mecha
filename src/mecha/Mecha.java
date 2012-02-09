package mecha;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;

import mecha.json.*;
import mecha.util.*;
import mecha.server.*;
import mecha.http.*;
import mecha.db.*;
import mecha.jinterface.*;
import mecha.vm.*;
import mecha.vm.channels.*;

public class Mecha {
    final private static Logger log = 
        Logger.getLogger(Mecha.class.getName());

    final public static JSONObject config;
    
    final private MVM mvm;
    final private MDB mdb;
    final private Server server;
    final private HTTPServer httpServer;
    final private SolrManager solrManager;
    final private RiakConnector riakConnector;
    final private RiakRPC riakRPC;
    final private Channels channels;
    final private EventLogManager eventLogManager;
    
    /*
     * startup & init
    */
    
    private static Mecha mechaInst;
    static {
        log.info("* reading config.json");
        config = loadConfig(TextFile.get("config.json"));
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
        log.info("* starting channels");
        channels = new Channels();

        log.info("* establishing riak link");
        riakRPC = new RiakRPC();
        introspectRiakConfig();
        
        log.info("* starting event log");
        eventLogManager = new EventLogManager();
    
        log.info("* starting solr cores");
        Logger.getLogger("org.apache.solr").setLevel(Level.WARNING);
        solrManager = new SolrManager();
        
        log.info("* starting mdb");
        mdb = new MDB();
        
        log.info("* starting mecha vm");
        mvm = new MVM();
        
        log.info("* script engines:");
        ScriptEngine.dumpScriptEngines();
        
        log.info("* starting socket server");
        server = new Server();
        
        log.info("* starting http server");
        httpServer = new HTTPServer();
        
        // log.info("* starting web services");
        // TODO: jetty ws integ
        
        log.info("* starting riak connector");
        riakConnector = new RiakConnector(mdb);
    }
    
    /*
     * use jinterface/erlang RPC to introspect
     *  required riak configuration values.
    */
    private void introspectRiakConfig() throws Exception {
        JSONObject riakConfig = config.getJSONObject("riak-config");
        riakConfig.put("local-url", riakRPC.getLocalRiakURL());
        riakConfig.put("pb-ip", riakRPC.getLocalPBIP());
        riakConfig.put("pb-port", riakRPC.getLocalPBPort());
        log.info("# introspected riak configuration: " + riakConfig.toString(2));
    }
    
    /*
     * post-initialization startup for components
     *  that depend on other components to exist
     *  and/or be initialized in some way.
    */
    public void start() throws Exception {
        startSolrCores();
        mdb.startMDB();
        riakConnector.startConnector();
        server.start();
        mvm.bootstrap();
        httpServer.start();
    }
    
    /* 
     * getters
    */
    
    public static Mecha get() {
        return mechaInst;
    }
    
    public static JSONObject getConfig() {
        return get().config;
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
    
    public static RiakRPC getRiakRPC() {
        return get().riakRPC;
    }
    
    public static Channels getChannels() {
        return get().channels;
    }
    
    public static MVM getMVM() {
        return get().mvm;
    }
    
    public static EventLogManager getEventLogManager() {
        return get().eventLogManager;
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
    
    /*
     * Widely used mechanism to generate cluster-wide (globally)
     *  unique identifiers.
    */
    final private static String GENERIC_GUID_TYPE = "guid";
    final private static String GUID_SEP = "/";
    
    /*
     * Generate an "untyped" (or "generic") guid.
    */
    public static String guid() throws Exception {
        return guid(GENERIC_GUID_TYPE);
    }
    
    /*
     * Generate a guid of a type that reflects the supplied
     *  class.
    */
    public static String guid(Class c) throws Exception {
        return guid(c.getName());
    }
    
    /*
     * Generate a "typed" guid.
     *
     * Currently the "host" portion of the guid is based on
     *  the protocol buffers IP configured for use within riak.
     *  This is in general the IP we want to use as it indicates
     *  the network address(es) we want to use for messaging
     *  between Mecha instances "java-side".
     *
    */
    public static String guid(String guidType) throws Exception {
        String nodeId = Mecha.getConfig()
                             .getJSONObject("riak-config")
                             .getString("pb-ip");
        return nodeId + GUID_SEP + UUID.randomUUID() + GUID_SEP + guidType;
    }
    
    /*
     * Get the host from which the supplied guid originated.
    */
    public static String getGuidHost(String guid) throws Exception {
        return guid.split(GUID_SEP)[0];
    }
    
    /*
     * Get the type of the guid supplied.
    */
    public static String getGuidType(String guid) throws Exception {
        return guid.split(GUID_SEP)[2];
    }
    
    /*
     * Universally useful helpers.
    */
    
    public static String[] exceptionToStringArray(Exception ex) {
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        ex.printStackTrace(printWriter);
        return result.toString().split("\n");
    }


    /*
     * Shutdown hook.
    */  
    
    static {
        Runtime.getRuntime().addShutdownHook(
            new Thread() {
                public void run() {
                    System.out.println("* shutting down...");
                    try {
                        System.out.println("* shutting down riak link");
                        System.out.println("* terminating socket server");
                        System.out.println("* terminating mvm");
                        System.out.println("* shutting down mdb");
                        System.out.println("* shutting down solr");
                        System.out.println("* shutdown complete");
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        log.info("* Error shutting down!  Repair will run on next start.");
                    }
                }
            });
    }
}
