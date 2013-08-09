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

package mecha.vm;

import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import javax.script.*;

import mecha.json.*;

public class ScriptEngine {
    final private static Logger log = 
        Logger.getLogger(ScriptEngine.class.getName());

    final private ScriptEngineManager manager;
    final private javax.script.ScriptEngine engine;
    final private Invocable invocableEngine;
    final private String vmType;
    final private boolean onlyEval;
    
	public ScriptEngine(String vmType) throws Exception {
        this.vmType = vmType;
        manager = new ScriptEngineManager();
        engine = manager.getEngineByName(this.vmType);
        if (engine instanceof Invocable) {
            invocableEngine = (Invocable) engine;
            onlyEval = false;
        } else {
            invocableEngine = null;
            onlyEval = true;
        }
    }

    public Object eval(String s) throws Exception {
        return engine.eval(s);
    }
    
    public Object invoke(String fn, Object... args) throws Exception {
        return invocableEngine.invokeFunction(fn, args);
    }
    
    public void bind(String var, Object val) throws Exception {
        engine.put(var, val);
    }
    
    public Object evalScript(String fn) throws Exception {
        InputStream is = 
            this.getClass().getResourceAsStream(fn);
        Reader reader = new InputStreamReader(is);
        return engine.eval(reader);
    }

    public static void dumpScriptEngines() throws Exception {
        ScriptEngineManager mgr = new ScriptEngineManager();
        List<ScriptEngineFactory> factories = 
            mgr.getEngineFactories();
        
        for (ScriptEngineFactory factory: factories) {
            log.info("ScriptEngineFactory Info");
            String engName = factory.getEngineName();
            String engVersion = factory.getEngineVersion();
            String langName = factory.getLanguageName();
            String langVersion = factory.getLanguageVersion();
            
            System.out.printf("\tScript Engine: %s (%s)\n", 
                engName, engVersion);
            
            List<String> engNames = factory.getNames();
            for(String name: engNames) {
                System.out.printf("\tEngine Alias: %s\n", name);
            }
            
            System.out.printf("\tLanguage: %s (%s)\n", 
                langName, langVersion);
        }
    }
}
