package mecha.client;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import jline.*;

public class MechaCLI {
    final private static Logger log = 
        Logger.getLogger(MechaCLI.class.getName());

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            log.info("usage: MechaCLI <ws://host:port/mecha> <password>");
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String pass = args[2];
        
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