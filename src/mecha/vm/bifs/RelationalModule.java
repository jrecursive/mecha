package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.lang.ref.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;
import java.text.Collator;

import mecha.Mecha;
import mecha.jinterface.*;
import mecha.util.HashUtils;
import mecha.json.*;
import mecha.vm.*;
import mecha.client.*;
import mecha.client.net.*;

public class RelationalModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(RelationalModule.class.getName());
    
    public RelationalModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
    }
    
    public void moduleUnload() throws Exception {
    }

    /*
     * Streaming sort-merge equijoin, a la 
     *  http://en.wikipedia.org/wiki/Sort-merge_join
     *
     * (sort-merge-equijoin left:(input:a field:f0) right:(input:b field:f1))
     *
    */    
    public class SortMergeEquiJoin extends MVMFunction {
        JSONObject left = null;
        JSONObject right = null;
        final private String leftInputVar;
        final private String rightInputVar;
        final private String leftOriginRefId;
        final private String rightOriginRefId;        
        final private String leftField;
        final private String rightField;
        private boolean leftDone;
        private boolean rightDone;
        private final Collator collator;
        
        public SortMergeEquiJoin(String refId, MVMContext ctx, JSONObject config) 
            throws Exception {
            super(refId, ctx, config);
            leftInputVar = config.getJSONObject("left").getString("input");
            rightInputVar = config.getJSONObject("right").getString("input");
            leftField = config.getJSONObject("left").getString("field");
            rightField = config.getJSONObject("right").getString("field");
            leftOriginRefId = getContext().resolveAssignmentToRefId(getContext().resolveVertexDelegate(leftInputVar));
            rightOriginRefId = getContext().resolveAssignmentToRefId(getContext().resolveVertexDelegate(rightInputVar));
            leftDone = false;
            rightDone = false;
            collator = Collator.getInstance();
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            String origin = msg.getString("$origin");
            if (!leftDone && origin.equals(leftOriginRefId)) {
                left = msg;
            } else if (!rightDone && origin.equals(rightOriginRefId)) {
                right = msg;
            } else {
                if (!leftDone && !rightDone) {
                    log.info("unknown origin ref id!  " + origin + " --> " + msg.toString());
                    return;
                }
            }
            if (left != null &&
                right != null) {
                final String leftValue = "" + left.get(leftField);
                final String rightValue = "" + right.get(rightField);
                int compareValue = collator.compare(leftValue, rightValue);
                
                if (compareValue == 0) {
                    JSONObject obj = new JSONObject();
                    String leftBucket = left.<String>get("bucket");
                    String rightBucket = right.<String>get("bucket");
                    for(String k : JSONObject.getNames(left)) {
                        if (k.equals("bucket")) continue;
                        if (k.startsWith("$")) continue;
                        obj.put(leftBucket + "." + k, left.get(k));
                    }
                    for(String k : JSONObject.getNames(right)) {
                        if (k.equals("bucket")) continue;
                        if (k.startsWith("$")) continue;
                        obj.put(rightBucket + "." + k, right.get(k));
                    }
                    broadcastDataMessage(obj);
                    advanceLeft();
                    advanceRight();
                    
                } else if (compareValue < 0) {
                    if (leftDone) {
                        broadcastDone();
                    } else {
                        advanceLeft();
                    }
                    
                } else if (compareValue > 0) {
                    if (rightDone) {
                        broadcastDone();
                    } else {
                        advanceRight();
                    }
                }
            } else {
                if (left == null) {
                    log.info("left null?");
                } else if (right == null) {
                    log.info("right null?");
                }
            }
            if (leftDone && rightDone) {
                broadcastDone();
            }
        }
        
        private void advanceLeft() throws Exception {
            if (leftDone) return;
            JSONObject nextMsg = new JSONObject();
            nextMsg.put("$", "next");
            Mecha.getMVM().nativeControlMessage(getContext(), leftInputVar, nextMsg);
        }

        private void advanceRight() throws Exception {
            if (rightDone) return;
            JSONObject nextMsg = new JSONObject();
            nextMsg.put("$", "next");
            Mecha.getMVM().nativeControlMessage(getContext(), rightInputVar, nextMsg);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            advanceLeft();
            advanceRight();
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            String origin = msg.getString("$origin");
            if (origin.equals(leftOriginRefId)) {
                if (left == null) {
                    rightDone = true;
                    log.info("No lefthand results.");
                } else {
                    log.info("<left-done> left: " + left.toString());
                }
                leftDone = true;
            } else if (origin.equals(rightOriginRefId)) {
                if (right == null) {
                    leftDone = true;
                    log.info("No righthand results.");
                } else {
                    log.info("<right-done> right: " + right.toString());
                }
                rightDone = true;
            } else {
                log.info("<done> unknown origin for done event! " + msg.toString());
            }
            if (leftDone || rightDone) {
                log.info("<done> " + msg.toString());
                broadcastDone(msg);
            }
        }
    }
    
}