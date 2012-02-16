package mecha.vm;

import java.lang.reflect.*; 
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.ericsson.otp.erlang.*;

import mecha.json.*;

import mecha.Mecha;
import mecha.util.*;
import mecha.db.*;
import mecha.vm.bifs.RiakClientModule;

public abstract class MVMModule {
    final private static Logger log = 
        Logger.getLogger(MVMModule.class.getName());
    
    final private String thisClassName;
        
    public MVMModule() throws Exception {
        thisClassName = this.getClass().getName();
    }

    public abstract void moduleLoad() throws Exception;
    public abstract void moduleUnload() throws Exception;
    
    public MVMFunction newFunctionInstance(MVMContext ctx,
                                           String funName, 
                                           String refId, 
                                           JSONObject initialState) throws Exception {
        final String funClassName =
            thisClassName + "$" + funName;
        Class funClass = Class.forName(funClassName);
        Class[] argTypes = { this.getClass(), String.class, MVMContext.class, JSONObject.class };
        Object[] args = { this, refId, ctx, initialState };
        try {
            return (MVMFunction) funClass.getConstructor(argTypes).newInstance(args);
        } catch (java.lang.reflect.InvocationTargetException ex) {
            log.info("** error creating new instance of mvm function '" + funName + "'");
            throw ex;
        }
    }
    
    public static MVMModule newModuleInstance(String moduleClassName) throws Exception {
        Class moduleClass = Class.forName(moduleClassName);
        Class[] argTypes = { };
        Object[] args = { };
        return (MVMModule) moduleClass.getConstructor(argTypes).newInstance(args);
    }
    
    public static void main(String args[]) throws Exception {
        RiakClientModule rc = new RiakClientModule();
        MVMFunction fun = rc.newFunctionInstance(null, "Get", "stub-refId-0", new JSONObject());
        fun.control(new JSONObject());
        fun.data(new JSONObject());
        
        rc.newFunctionInstance(null, "Put", "stub-refId-1", new JSONObject()).control(new JSONObject());
    }
    
}