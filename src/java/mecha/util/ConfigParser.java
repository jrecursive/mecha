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
 * 
 * (C) 2013 John Muellerleile @jrecursive
 *
 */

package mecha.util;

import java.util.logging.*;
import mecha.json.*;

public class ConfigParser {
    final private static Logger log = 
        Logger.getLogger(ConfigParser.class.getName());

    public static JSONObject parseConfig(String configFileStr) {
        if (configFileStr == null) {
            log.info("there must be a config file named 'config.json'");
            System.exit(-1);
        }
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