package mecha.server;

import java.util.*;

/*
 * messaging channel
*/
public class PubChannel {
    public String name;
    public Set<Client> members;
    
    public PubChannel(String name) {
        this.name = name;
        members = Collections.synchronizedSet(new HashSet());
    }
}
