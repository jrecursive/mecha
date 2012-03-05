package mecha.util;

import java.util.*;
import java.net.*;
import java.io.*;

public class HTTPUtils {
    public static Map<String, List<String>> getURLHeaders(String u) throws Exception {
        URL url = new URL(u);
        HttpURLConnection.setFollowRedirects(true);
        HttpURLConnection uc = (HttpURLConnection) url.openConnection();
        uc.setConnectTimeout(5000);
        uc.setReadTimeout(5000);
        uc.setRequestMethod("HEAD");
        uc.setRequestProperty("Connection", "Close");
        uc.connect();
        BufferedReader in = new BufferedReader(new InputStreamReader(uc.getInputStream()));
        in.close();
        uc.disconnect();
        Map<String, List<String>> hdrs = uc.getHeaderFields();
        in=null;
        uc=null;
        url=null;
        return hdrs;
    }
    
    @SuppressWarnings("deprecation")
    public static String fetch(String u) throws Exception {
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

}
