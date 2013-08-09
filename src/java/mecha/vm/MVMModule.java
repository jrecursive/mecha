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
            Mecha.getMonitoring().error("mecha.vm.mvm", ex);
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