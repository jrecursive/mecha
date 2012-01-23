package mecha.vm;

import java.lang.reflect.*; 
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.ericsson.otp.erlang.*;
import org.json.*;

import mecha.Mecha;
import mecha.util.*;
import mecha.db.*;

public abstract class MVMModule {
    final private static Logger log = 
        Logger.getLogger(MVMModule.class.getName());

    public MVMModule() throws Exception {
        
    }

    public abstract void load() throws Exception;
    public abstract void unload() throws Exception;
}