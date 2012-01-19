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
    
    public static void main(String args[]) throws Exception {
        Mecha mecha = new Mecha();
    }
    
    public static JSONObject getConfig() throws Exception {
        return config;
    }
    
    private Mecha() throws Exception {
        log.info("starting indexes");
        solrManager = new SolrManager();
        
        log.info("starting mdb");
        mdb = new MDB();
        
        log.info("starting query server");
        server = new Server();
        server.start();
        
        log.info("starting web services");
        // TODO: jetty ws integ
        
        log.info("starting riak connector");
        riakConnector = new RiakConnector(mdb);
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
