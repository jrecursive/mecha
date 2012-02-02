package mecha.client;

import mecha.json.*;

public abstract class MechaClientHandler {
    
    public void onSystemMessage(JSONObject msg) throws Exception {
        System.out.println("<system> " + msg.toString(2));
    }
    
    public void onOpen() throws Exception {
        System.out.println("<connected>");
    }

    public void onClose() throws Exception {
        System.out.println("<disconnected>");
    }

    public void onError(Exception ex) {
        System.out.println("<error> " + ex.toString());
        ex.printStackTrace();
    }
    
    public abstract void onMessage(String msg);

    public void onDataMessage(String channel, JSONObject msg) throws Exception {
        System.out.println("<data: " + channel + "> " + msg.toString(2));
    }

    public void onDoneEvent(String channel, JSONObject msg) throws Exception {
        System.out.println("<done: " + channel + "> " + msg.toString(2));
    }
    
    public void onControlEvent(String channel, JSONObject msg) throws Exception {
        System.out.println("<control: " + channel + "> " + msg.toString(2));
    }
    
    public void onOk(String msg) throws Exception {
        System.out.println("<ok> " + msg);
    }
    
    public void onInfo(String msg) throws Exception {
        System.out.println("<info> " + msg);
    }
    
}
