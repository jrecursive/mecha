package mecha.vm;

import java.util.*;

public class MacroUtils {
    public String escapeQuotes(String s) throws Exception {
        if (s.indexOf("\\\"") != -1) return s;
        return s.replaceAll("\"", "\\\\\"");
    }
}
