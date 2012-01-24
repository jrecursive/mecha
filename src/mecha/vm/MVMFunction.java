package mecha.vm;

import java.util.*;
import java.util.logging.*;

import org.json.*;

public abstract class MVMFunction {
    final private static Logger log = 
        Logger.getLogger(MVMFunction.class.getName());

        public MVMFunction(JSONObject config) throws Exception {
            log.info("new " + this.getClass().getName() + "()");
        }
        
        public abstract void control(JSONObject msg) throws Exception;
        public abstract void data(JSONObject msg) throws Exception;
}
