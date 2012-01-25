package mecha.vm;

import java.util.*;

public class RegisteredFunction {
    
    final private String verb;
    final private String verbInnerClassName;
    final private String owningModuleClassName;
    
    public RegisteredFunction(String verb,
                              String verbInnerClassName,
                              String owningModuleClassName) {
        this.verb = verb;
        this.verbInnerClassName = verbInnerClassName;
        this.owningModuleClassName = owningModuleClassName;
    }
    
}
