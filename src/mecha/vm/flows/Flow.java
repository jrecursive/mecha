package mecha.vm.flows;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.jgrapht.*;
import org.jgrapht.graph.*;

import mecha.Mecha;
import mecha.util.*;
import mecha.json.*;
import mecha.vm.*;

public class Flow {
    final private static Logger log = 
        Logger.getLogger(Flow.class.getName());
    
    /*
     * Used as fields in data payloads for
     *  vertices & edges
    */
    final public static String CLIENT_ID = "cid";
    final public static String REF_ID = "refid";
    final public static String CONTEXT_REF_ID = "ctx-refid";
    final public static String EXPR = "expr";
    final public static String CTX_VAR = "ctx-var";
    
    /*
     * Edge labels
    */
    final public static String FLOW_EDGE_REL = "flows-to";
    
    /*
     * Flow graph
    */
    final private ListenableDirectedWeightedGraph<Vertex, Edge> graph;
    final private ConcurrentHashMap<String, Vertex> vertices;
    final private ConcurrentHashMap<String, Edge> edges;
    
    public Flow() {
        graph = new ListenableDirectedWeightedGraph<Vertex, Edge>(Edge.class);
        vertices = new ConcurrentHashMap<String, Vertex>();
        edges = new ConcurrentHashMap<String, Edge>();
    }
    
    /*
     * Vertex operations
    */
    
    public void addVertex(String refId, JSONObject obj) throws Exception {
        Vertex v = new Vertex(refId, obj);
        graph.addVertex(v);
        vertices.put(refId, v);
    }
    
    public Vertex getVertex(String refId) throws Exception {
        return vertices.get(refId);
    }
    
    public boolean removeVertex(String refId) throws Exception {
        return removeVertex(getVertex(refId));
    }
    
    public boolean removeVertex(Vertex vertex) throws Exception {
        if (graph.removeVertex(vertex)) {
            vertices.remove(vertex.<String>get(REF_ID));
            return true;
        }
        return false;
    }
    
    /*
     * Edge operations
    */
    
    public void addEdge(String refId, 
                        JSONObject obj,
                        String vKeyFrom, 
                        String vKeyTo, 
                        String rel, 
                        double weight) throws Exception {
        Vertex fromVertex = getVertex(vKeyFrom);
        Vertex toVertex   = getVertex(vKeyTo);
        Edge<Vertex> edge = new Edge<Vertex>(refId, 
                                             fromVertex, 
                                             toVertex, 
                                             rel,
                                             obj);
        graph.addEdge(fromVertex, toVertex, edge);
        graph.setEdgeWeight(edge, weight);
    }
    
    public Edge getEdge(String refId) throws Exception {
        return edges.get(refId);
    }
    
    public boolean removeEdge(Edge edge) throws Exception {
        if (graph.removeEdge(edge)) {
            edges.remove((String)edge.get(REF_ID));
            return true;
        }
        return false;
    }

    public void setEdgeWeight(Edge edge, double weight) throws Exception {
        graph.setEdgeWeight(edge, weight);
    }
    
    public double getEdgeWeight(Edge edge) throws Exception {
        return graph.getEdgeWeight(edge);
    }

    /*
     * Neighborhood operations
    */
    
    // outgoing edges of
    
    public Set<Edge> getOutgoingEdgesOf(String refId) throws Exception {
        return getOutgoingEdgesOf(getVertex(refId));
    }
    
    public Set<Edge> getOutgoingEdgesOf(Vertex vertex) throws Exception {
        return graph.outgoingEdgesOf(vertex);
    }
    
    // outgoing neighbors of
    
    public Set<Vertex> getOutgoingNeighborsOf(String refId) throws Exception {
        return getOutgoingNeighborsOf(getVertex(refId));
    }
    
    public Set<Vertex> getOutgoingNeighborsOf(String refId, String rel) throws Exception {
        return getOutgoingNeighborsOf(getVertex(refId), rel);
    }
    
    public Set<Vertex> getOutgoingNeighborsOf(String refId, Set<String> rels) throws Exception {
        return getOutgoingNeighborsOf(getVertex(refId), rels);
    }
    
    public Set<Vertex> getOutgoingNeighborsOf(Vertex vertex) throws Exception {
        Set<String> rels = null;
        return getOutgoingNeighborsOf(vertex, rels);
    }
    
    public Set<Vertex> getOutgoingNeighborsOf(Vertex vertex, String rel) throws Exception {
        Set<String> rels = new HashSet<String>();
        rels.add(rel);
        return getOutgoingNeighborsOf(vertex, rels);
    }
    
    public Set<Vertex> getOutgoingNeighborsOf(Vertex vertex, Set<String> rels) throws Exception {
        Set<Edge> edges = getOutgoingEdgesOf(vertex);
        Set<Vertex> neighbors = new HashSet<Vertex>();
        for(Edge edge: edges) {
            if (rels != null) {
                String edgeRel = edge.getRel();
                if (rels.contains(edgeRel)) {
                    neighbors.add((Vertex) edge.getTarget());
                }
            } else {
                neighbors.add((Vertex) edge.getTarget());
            }
        }
        return neighbors;
    }
    
    // incoming edges of
    
    public Set<Edge> getIncomingEdgesOf(Vertex vertex) throws Exception {
        return graph.incomingEdgesOf(vertex);
    }
    
    // incoming neighbors of

    public Set<Vertex> getIncomingNeighborsOf(Vertex vertex) throws Exception {
        Set<String> rels = null;
        return getIncomingNeighborsOf(vertex, rels);
    }
    
    public Set<Vertex> getIncomingNeighborsOf(Vertex vertex, String rel) throws Exception {
        Set<String> rels = new HashSet<String>();
        rels.add(rel);
        return getIncomingNeighborsOf(vertex, rels);
    }
    
    public Set<Vertex> getIncomingNeighborsOf(Vertex vertex, Set<String> rels) throws Exception {
        Set<Edge> edges = getIncomingEdgesOf(vertex);
        Set<Vertex> neighbors = new HashSet<Vertex>();
        for(Edge edge: edges) {
            if (rels != null) {
                String edgeRel = edge.getRel();
                if (rels.contains(edgeRel)) {
                    neighbors.add((Vertex) edge.getTarget());
                }
            } else {
                neighbors.add((Vertex) edge.getTarget());
            }
        }
        return neighbors;
    }
    
    
    /*
     * Misc helpers
    */
    
    public int vertexCount() throws Exception {
        return vertices.size();
    }
    
    public int edegCount() throws Exception {
        return graph.edgeSet().size();
    }

}