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
import mecha.monitoring.*;
import mecha.riak.*;

public class Mecha {
    final private static Logger log = 
        Logger.getLogger(Mecha.class.getName());

    final public static JSONObject config;
    final public static long startTime = 
        System.currentTimeMillis();
    
    final private MVM mvm;
    final private MDB mdb;
    final private Server server;
    final private HTTPServer httpServer;
    final private SolrManager solrManager;
    final private RiakManager riakManager;
    final private RiakConnector riakConnector;
    final private RiakRPC riakRPC;
    final private Channels channels;
    final private EventLogManager eventLogManager;
    final private Monitoring monitoring;
    static private boolean shuttingDown = false;
    
    /*
     * startup & init
    */
    
    private static Mecha mechaInst;
    static {
        log.info("* reading config.json");
        config = ConfigParser.parseConfig(TextFile.get("config.json"));
        log.info("* generating riak config");
        generateRiakConfigs(config);
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
        log.info("* starting monitoring");
        monitoring = new Monitoring();

        riakManager = new RiakManager();
        if (Mecha.getConfig().<Boolean>get("riak-start")) {
            System.out.println("* starting riak_kv");
            Mecha.ensureRiak(riakManager);
        } else {
            System.out.println("! not forcing erlang shutdown !");
        }
    
        log.info("* starting channels");
        channels = new Channels();

        log.info("* establishing riak link");
        riakRPC = new RiakRPC();
        introspectRiakConfig();
        
        log.info("* starting event log");
        eventLogManager = new EventLogManager();
    
        log.info("* starting solr cores");
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
        
        log.info("* starting riak connector");
        riakConnector = new RiakConnector(mdb);
        
        // TODO: fix init problem
        
    }
    
    /*
     * use jinterface/erlang RPC to introspect
     *  required riak configuration values.
    */
    private void introspectRiakConfig() throws Exception {
        JSONObject riakConfig = config.getJSONObject("riak-config");
        riakConfig.put("local-url", riakRPC.getLocalRiakURL());
        riakConfig.put("pb-ip", riakRPC.getLocalPBIP());
        config.put("server-addr", riakConfig.<String>get("pb-ip"));
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
        monitoring.start();
        channels.start();
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
    
    public static Monitoring getMonitoring() {
        return get().monitoring;
    }
    
    public static RiakManager getRiakManager() {
        return get().riakManager;
    }
    
    /*
     * helpers
    */
    
    private static void startSolrCores() throws Exception {
        System.setProperty("solr.data.dir", getConfig().getString("solr-data-dir"));
        System.setProperty("solr.tmp.data.dir", getConfig().getString("solr-temporary-data-dir"));
        System.setProperty("solr.system.data.dir", getConfig().getString("solr-system-data-dir"));
        JSONArray cores = getConfig().getJSONArray("solr-cores");
        for(int i=0; i<cores.length(); i++) {
            String coreName = cores.getString(i);
            log.info("/ starting core: " + coreName);
            if (null == get().solrManager.startCore(coreName)) {
                throw new Exception ("Cannot start solr core '" + coreName);
            }
        }
    }
    
    private static void generateRiakConfigs(JSONObject config) {
        try {
            String riakHome = config.<String>get("riak-home");
            String appTemplateFn = 
                riakHome + "/" + config.<String>get("riak-app-config-template");
            String vmTemplateFn = 
                riakHome + "/" + config.<String>get("riak-vm-config-template");
            String appConfigFn = 
                riakHome + "/" + config.<String>get("riak-app-config");
            String vmConfigFn = 
                riakHome + "/" + config.<String>get("riak-vm-config");
            log.info("* writing " + appConfigFn + " via " + appTemplateFn);
            TextFile.put(appConfigFn, 
                         makeRiakConfig(TextFile.get(appTemplateFn), config));
            log.info("* writing " + vmConfigFn + " via " + vmTemplateFn);
            TextFile.put(vmConfigFn, 
                         makeRiakConfig(TextFile.get(vmTemplateFn), config));
            log.info("* riak config generation complete");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
        }
    }
    
    private static String makeRiakConfig(String str, JSONObject config) throws Exception {
        StringBuffer cfsb = new StringBuffer();
        try {
            String[] configFileStrLines = str.split("\n");
            for(String configFileLine : configFileStrLines) {
                String cleanLine = configFileLine;
                
                int idx;
                while((idx = cleanLine.indexOf("<<mecha:"))!=-1) {
                    int idx1 = cleanLine.indexOf(">>", idx);
                    String line0 = cleanLine.substring(0,idx);
                    String line1 = cleanLine.substring(idx1+2);
                    String varName = cleanLine.substring(idx+8, idx1).trim();
                    cleanLine = line0 + config.get(varName) + line1;
                }
                cfsb.append(cleanLine);
                cfsb.append("\n");
            }
            return cfsb.toString();
        } catch (Exception configException) {
            configException.printStackTrace();
            log.info("could not parse riak config template file!  processed version: ");
            log.info(cfsb.toString());
            throw configException;
        }
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
        
        String[] parts = c.getName().split("\\.");
        return guid(parts[parts.length-1]);
        
        //return guid(c.getName());
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
                             .getString("server-addr");
        return HashUtils.sha1(nodeId + GUID_SEP + UUID.randomUUID()) + GUID_SEP + guidType;
        //return nodeId + GUID_SEP + UUID.randomUUID() + GUID_SEP + guidType;
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
    
    public static String getHost() throws Exception {
        return Mecha.getConfig().getString("server-addr");
    }
    
    public static String getLocalRiakURL() throws Exception {
        return Mecha.getConfig().getJSONObject("riak-config").getString("local-url");
    }
    
    public static JSONObject getRiakConfig() throws Exception {
        return Mecha.getConfig().getJSONObject("riak-config");
    }
    
    /*
     * Riak management
    */
    
    public static void riakDown() throws Exception {
        if (shuttingDown ||
            !Mecha.getConfig().<Boolean>get("riak-start")) {
            return;
        }
        log.info("restarting riak (not responding)");
        ensureRiak(Mecha.getRiakManager());
    }
    
    private static synchronized void ensureRiak(RiakManager riakManager) throws Exception {
        log.info("starting riak");
        String response;
        while(true) {
            try {
                response = riakManager.riakCommand("ping");
                if (response.indexOf("not responding to pings") != -1) {
                    response = riakManager.riakCommand("start");
                    if (response.indexOf("Node is already running!") != -1) {
                        log.info("Node already running?  Retrying ping in 1 second.");
                        continue;
                    } else {
                        /* 
                         * there is no indicator (other than lack of "Node is already running")
                         *  for a successful node start, so let's continue through and 
                         *  the ping should succeed.
                        */
                        log.info("riak seems to have started... retrying ping");
                        continue;
                    }
                } else if (response.indexOf("pong") != -1) {
                    log.info("riak responded!");
                    return;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                Thread.sleep(1000);
            }
        }
    }
    
    /*
     * Shutdown hook.
    */
    
    public static boolean isShuttingDown() {
        return shuttingDown;
    }
    
    public static void shuttingDown() {
        shuttingDown = true;
    }
    
    static {
        Runtime.getRuntime().addShutdownHook(
            new Thread() {
                public void run() {
                    Mecha.shuttingDown();
                    
                    System.out.println("* shutting down...");
                    try {
                        Mecha.getMonitoring().stop();
                        if (Mecha.getConfig().<Boolean>get("riak-stop")) {
                            System.out.println("* stopping riak_kv");
                            Mecha.getRiakManager().riakCommand("stop");
                            //Mecha.getRiakRPC().shutdown();
                        } else {
                            System.out.println("! not forcing erlang shutdown !");
                        }
                        
                        System.out.println("* terminating socket server");
                        Mecha.getServer().shutdown();
                        
                        System.out.println("* shutting down mdb");
                        Mecha.getMDB().shutdown();
                        
                        System.out.println("* shutting down solr");
                        Mecha.getSolrManager().shutdown();
                        
                        System.out.println("* shutdown complete");
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        log.info("* Error shutting down!  Repair will run on next start.");
                    }
                }
            });
    }
}
