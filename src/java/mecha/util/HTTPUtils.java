/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package mecha.util;

import java.util.*;
import java.net.*;
import java.io.*;

public class HTTPUtils {
    
    static class RiakAdminAuthenticator extends Authenticator {
        public PasswordAuthentication getPasswordAuthentication() {
            System.out.println("Authenticating: " + getRequestingScheme());
            return (new PasswordAuthentication("mecha", "mecha".toCharArray()));
        }
    }
    
    static {
        Authenticator.setDefault(new RiakAdminAuthenticator());
    }

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
