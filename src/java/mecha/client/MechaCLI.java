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

package mecha.client;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import jline.*;

import mecha.json.JSONObject;
import mecha.util.TextFile;
import mecha.util.ConfigParser;

public class MechaCLI {
    final private static Logger log = 
        Logger.getLogger(MechaCLI.class.getName());

    public static void main(String[] args) throws Exception {
        log.info("* reading config.json");
        JSONObject config = ConfigParser.parseConfig(TextFile.get("config.json"));
        String host = config.<String>get("server-addr");
        int port = config.<Integer>get("server-port");
        String pass = config.<String>get("password");
        
        MechaClient client = new MechaClient(host, port, pass);
        
        ConsoleReader reader = new ConsoleReader();
        reader.setBellEnabled(false);
        
        String line;        
        while ((line = reader.readLine("mecha-cli> ")) != null) {
            line = line.trim();
            if (line.equals("")) continue;
            client.exec(line);
        }
    }
}