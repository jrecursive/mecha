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

package mecha.cep;

import java.util.*;
import mecha.json.*;

public class CEvent {
    public String type;
    public JSONObject obj;
    
    public CEvent(String type, JSONObject obj) {
        this.type = type;
        this.obj = obj;
    }
    
    public String getType() {
        return type;
    }
    
    public JSONObject getObj() {
        return obj;
    }
}
