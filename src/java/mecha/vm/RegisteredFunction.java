package mecha.vm;

import java.util.*;

public class RegisteredFunction {
    
    final private String verb;
    final private String verbClassName;
    final private String moduleClassName;
    
    public RegisteredFunction(String verb,
                              String verbClassName,
                              String moduleClassName) {
        this.verb = verb;
        this.verbClassName = verbClassName;
        this.moduleClassName = moduleClassName;
    }
    
    public String getVerb() { return verb; }
    
    public String getVerbClassName() { return verbClassName; }
    
    public String getModuleClassName() { return moduleClassName; }
    
}
