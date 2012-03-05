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