package mecha.util;

import java.io.*;
import java.util.logging.*;
import java.net.*;

import org.json.*;

public class RiakUtils {
    final private static Logger log = 
        Logger.getLogger(RiakUtils.class.getName());

    // TODO: configuration proper
    final private String[] riakUrls =
        { "http://api1.tx:8098/riak",
          "http://api2.tx:8098/riak",
          "http://api3.tx:8098/riak",
          "http://api4.tx:8098/riak",
          "http://api5.tx:8098/riak",
          "http://api7.tx:8098/riak"
         };
    private int serverIdx = 0;
    
    private String getRiakUrl() throws Exception {
        serverIdx++;
        if (serverIdx > (riakUrls.length-1)) serverIdx = 0;
        return riakUrls[serverIdx];
    }
    
    @SuppressWarnings("deprecation")
    private String geturl(String u) throws Exception {
        URL url = new URL(u);
        InputStream is = url.openStream();
        DataInputStream dis = new DataInputStream(new BufferedInputStream(is));
        StringBuffer sb = new StringBuffer();
        String s;
        while ((s = dis.readLine()) != null) {
            sb.append(s);
            sb.append("\n");
        }
        dis.close();
        url = null; 
        return sb.toString().trim();
    }
    
    public JSONObject riakGet(String bucket, String key) throws Exception {
        return riakGet(bucket, key, 1);
    }
    
    public JSONObject riakGet(String bucket, String key, int r) throws Exception {
        while(true) {
            try {
                String s = geturl(getRiakUrl() + "/" + bucket + "/" + key + "?r=" + r);
                if (s.startsWith("{")) {
                    return new JSONObject(s);
                } else {
                    if (s.indexOf("Siblings") != -1) {
                        log.info(s);
                        String[] siblings = s.replace("Siblings:", "").trim().split("\n");
                        for(String sibling: siblings) {
                            log.info("sibling: " + sibling);
                        }
                        return riakGet(bucket, key + "&vtag=" + siblings[0]);
                    } else {
                        log.info("wtf:\n" + s);
                        throw new Exception("not json " + s);
                    }
                }
            } catch (java.net.ConnectException ex) {
                Thread.sleep(1000);
                log.info("retrying " + bucket + ": " + key + " / " + r);
            } catch (java.net.SocketException ex1) {
                Thread.sleep(1000);
                log.info("retrying " + bucket + ": " + key + " / " + r);
            }
        }
    }
    
}