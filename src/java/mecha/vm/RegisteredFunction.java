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
