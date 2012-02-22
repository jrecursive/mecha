package mecha.riak;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;

import mecha.Mecha;
import mecha.json.*;

public class RiakManager {
    final private static Logger log = 
        Logger.getLogger(RiakManager.class.getName());
    
    public String riakCommand(String... args) throws Exception {
        return doCommand(Mecha.getConfig().<String>get("riak-script"),
                         args);
    }

    public String riakAdminCommand(String... args) throws Exception {
        return doCommand(Mecha.getConfig().<String>get("riak-admin-script"),
                         args);
    }
    
    public String doCommand(String riakScript, String... args) throws Exception {
        List<String> riakProcessArgs = 
            new ArrayList<String>();
        String riakBasePath = Mecha.getConfig().<String>get("riak-home");
        
        //String bashBinaryPath = Mecha.getConfig().<String>get("bash-binary");
        //riakProcessArgs.add(bashBinaryPath);
        
        riakProcessArgs.add(riakScript);
        for(String arg : args) {
            riakProcessArgs.add(arg);
        }
        Process riakProcess = 
            new ProcessBuilder(riakProcessArgs)
                .redirectErrorStream(true)
                .directory(new File(riakBasePath))
                .start();
        String response = logUntilCompletion(riakProcess, args[0]);
        riakProcess.destroy();
        return response;
    }
    
    private String logUntilCompletion(Process process, String command) throws Exception {
        String line;
        StringBuffer response = new StringBuffer();
        InputStream processInputStream = 
            process.getInputStream();
        BufferedReader logReader = 
            new BufferedReader(
                new InputStreamReader(
                    process.getInputStream()));
        while ((line = logReader.readLine()) != null) {
            log.info("<" + command + "> " + line);
            /*
             * To protect against NPEs caused by 
             *  riakCommand calls at startup.
            */
            if (Mecha.get() != null) {
                Mecha.getMonitoring()
                     .log("mecha.riak-manager." + command,
                          line);
            }
            response.append(line + "\n");
        }
        processInputStream.close();
        return response.toString();
    }
    

}