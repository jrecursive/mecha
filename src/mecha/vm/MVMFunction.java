package mecha.vm;

import java.util.*;
import java.util.logging.*;

import org.jetlang.channels.*;
import org.jetlang.core.Callback;

import mecha.json.*;

public abstract class MVMFunction {
    final private static Logger log = 
        Logger.getLogger(MVMFunction.class.getName());
    
    /*
     * Flags to denote message typing & content, e.g.,
     *  control, data, etc.
    */
    final private static String MESSAGE_TYPE_FIELD = "t";
    final private static String CONTROL_CHANNEL    = "c";
    final private static String DATA_CHANNEL       = "d";
    final private static String OBJECT_FIELD       = "o";
    
    /*
     * to be returned via overridden getAffinity()
     *  method.
    */
    final private static int NO_AFFINITY        = 0;
    final private static int PRODUCER_AFFINITY  = 1;
    final private static int CONSUMER_AFFINITY  = 2;
    
    /*
     * Context with which the instance of the function was constructed.
    */
    final private MVMContext context;
    
    public MVMFunction(MVMContext context, 
                       JSONObject config) throws Exception {
        this.context = context;
        log.info("<constructor> " + this.getClass().getName());
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
