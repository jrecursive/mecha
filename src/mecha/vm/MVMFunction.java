package mecha.vm;

import java.util.*;
import java.util.logging.*;

import org.jetlang.channels.*;
import org.jetlang.core.Callback;

import org.json.*;

public abstract class MVMFunction {
    final private static Logger log = 
        Logger.getLogger(MVMFunction.class.getName());
        
    final private static String MESSAGE_TYPE_FIELD = "t";
    final private static String CONTROL_CHANNEL    = "c";
    final private static String DATA_CHANNEL       = "d";
    final private static String OBJECT_FIELD       = "o";
    
    private MVMContext context = null;
    
    public MVMFunction(JSONObject config) throws Exception {
        log.info("<constructor> " + this.getClass().getName());
    }
    
    protected void setMVMContext(MVMContext context) throws Exception {
        if (this.context != null) {
            throw new Exception("context already set!");
        }
        this.context = context;
    }
    
    public MVMContext getContext() {
        return context;
    }
    
    public Callback<JSONObject> getCallback() throws Exception {
        return new Callback<JSONObject>() {
            public void onMessage(JSONObject message) {
                try {
                    if (message.has(MESSAGE_TYPE_FIELD)) {
                        if (message.getString(MESSAGE_TYPE_FIELD)
                                   .equals(CONTROL_CHANNEL)) {
                            control(message.getJSONObject(OBJECT_FIELD));
                        } else if (message.getString(MESSAGE_TYPE_FIELD)
                                          .equals(DATA_CHANNEL)) {
                            data(message.getJSONObject(OBJECT_FIELD));
                        } else {
                            info(message);
                        }
                    } else {
                        info(message);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };
    }
    
    public void control(JSONObject msg) throws Exception {
        log.info("<control> " + this.getClass().getName() + ": " +
            msg.toString(2));
    }

    public void data(JSONObject msg) throws Exception {
        log.info("<data> " + this.getClass().getName() + ": " +
            msg.toString(2));
    }

    public void info(JSONObject msg) throws Exception {
        log.info("info: " + msg.toString(2));
    }
}
