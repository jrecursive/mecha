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
    
    public MVMFunction newFunctionInstance(String funName, JSONObject initialState) throws Exception {
        final String funClassName =
            thisClassName + "$" + funName;
        Class funClass = Class.forName(funClassName);
        Class[] argTypes = { this.getClass(), JSONObject.class };
        Object[] args = { this, initialState };
        return (MVMFunction) funClass.getConstructor(argTypes).newInstance(args);
    }
    
    public static MVMModule newModuleInstance(String moduleClassName) throws Exception {
        Class moduleClass = Class.forName(moduleClassName);
        Class[] argTypes = { };
        Object[] args = { };
        return (MVMModule) moduleClass.getConstructor(argTypes).newInstance(args);
    }
    
    public static void main(String args[]) throws Exception {
        RiakClientModule rc = new RiakClientModule();
        MVMFunction fun = rc.newFunctionInstance("Get", new JSONObject());
        fun.control(new JSONObject());
        fun.data(new JSONObject());
        
        rc.newFunctionInstance("Put", new JSONObject()).control(new JSONObject());
    }
    
}