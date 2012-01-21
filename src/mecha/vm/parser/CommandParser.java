package mecha.vm.parser;

import java.util.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;

import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.*;
import org.apache.lucene.queryParser.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.util.Version;
import org.apache.lucene.index.Term;

import org.json.*;

import org.jgrapht.*;
import org.jgrapht.graph.*;
import org.jgrapht.ext.*;

public class CommandParser {
    final private static Logger log = 
        Logger.getLogger(CommandParser.class.getName());
    
	public boolean SHOW_GRAPH = false;
	final private static String DEFAULT_SEARCH_FIELD = "default";
	private ListenableDirectedGraph<Vertex, Edge> queryGraph;
	private Vertex root = new Vertex("root");
	private Vertex parentVertex = root;
	
	final private static String _ARGS = "_ARGS";
	final private static String _TYPE = "_TYPE";
	
	final private static String _OP_T = "op";
	final private static String _VALUE_T = "value";
	final private static String _RANGE_T = "range";
	
	private JSONObject qry = new JSONObject();
	
	public ListenableDirectedGraph<Vertex, Edge> parse(String query) throws Exception {
		return parse(DEFAULT_SEARCH_FIELD, query);
	}
	
	public ListenableDirectedGraph<Vertex, Edge> 
	   parse(String defaultField, String query) throws Exception {
		queryGraph = new ListenableDirectedGraph<Vertex,Edge>(Edge.class);
		root = new Vertex("root");
		addVertex(root);
		parentVertex = root;
		
		Analyzer analyzer = new WhitespaceAnalyzer();
		QueryParser luceneParser = new QueryParser(Version.LUCENE_35, defaultField, analyzer);
		luceneParser.setDefaultOperator(QueryParser.Operator.OR);
		luceneParser.setAllowLeadingWildcard(true);
		Query luceneQuery = luceneParser.parse(query);
		
		qry = newQueryObj(_OP_T);
		process(luceneQuery, qry);
        
        log.info("\n");
        log.info(qry.toString(4));
        
		if (SHOW_GRAPH) {
	       	FileWriter outFile = new FileWriter("/tmp/query.dot");
	       	PrintWriter dot_wr = new PrintWriter(outFile);
	       	DOTExporter de = new DOTExporter(
	       	   new VertexNameProvider<Vertex>() {
	       	       public String getVertexName(Vertex v) {
	       	           try {
    	       	           return "" + v.vertexDotId;
    	       	       } catch (Exception ex) {
    	       	           ex.printStackTrace();
    	       	           return null;
    	       	       }
	       	       }
	       	   },
	       	   new VertexNameProvider<Vertex>() {
	       	       public String getVertexName(Vertex v) {
	       	           return v.toString();
	       	       }
	       	   }, 
	       	   new EdgeNameProvider<Edge>() {
	       	       public String getEdgeName(Edge e) {
	       	           return e.toString();
	       	       }
	           },
	           new ComponentAttributeProvider<Vertex>() {
	               public java.util.Map<java.lang.String,java.lang.String> getComponentAttributes(Vertex v) {
	                   return new java.util.HashMap<java.lang.String,java.lang.String>();
	               }
	           },
	           new ComponentAttributeProvider<Edge>() {
	               public java.util.Map<java.lang.String,java.lang.String> getComponentAttributes(Edge e) {
	                   return new java.util.HashMap<java.lang.String,java.lang.String>();
	               }	               
	           }
            );
	       	de.export(dot_wr, queryGraph);
	       	outFile.close();
		}
		
		return queryGraph;
	}
	
    // temporary
    private int depth = 0;
    private void indent() {
        for (int i=0; i<depth; i++) {
            System.out.print("    ");
        }
    }
    private void dbg(String s) {
        indent();
        System.out.println(s);
    }
    
    /*
     * graph
    */
    
    private void addVertex(Vertex v) {
        queryGraph.addVertex(v);
    }
    
    private void addEdge(Vertex from, Vertex to, String rel) {
        Edge<Vertex> e = new Edge<Vertex>(from, to, rel);
        queryGraph.addEdge(from, to, e);
    }
    
    private ListenableDirectedGraph<Vertex,Edge> getGraph() {
        return queryGraph;
    }
    
    private JSONObject newQueryObj(String type) throws Exception {
        JSONObject obj = new JSONObject();
        obj.put(_TYPE, type);
        obj.put(_ARGS, new JSONArray());
        return obj;
    }
    
    private JSONArray getArgs(JSONObject obj) throws Exception {
        return obj.getJSONArray(_ARGS);
    }
    
    /*
     * resolver
    */
    
    //
    // INFO: query (org.apache.lucene.search.BooleanQuery) $:cmd (param1:value1 param2:value2)
    //
    private void process(Query qel, JSONObject qobj) throws Exception {
        if (qel instanceof BooleanQuery)
            process((BooleanQuery) qel, qobj);
        else if (qel instanceof TermQuery)
            process((TermQuery) qel, qobj);
        else if (qel instanceof MultiTermQuery)
            process((MultiTermQuery) qel, qobj);
        else if (qel instanceof WildcardQuery)
            process((WildcardQuery) qel, qobj);
        else if (qel instanceof PhraseQuery)
            process((PhraseQuery) qel, qobj);
        else if (qel instanceof PrefixQuery)
            process((PrefixQuery) qel, qobj);
        else if (qel instanceof MultiPhraseQuery)
            process((MultiPhraseQuery) qel, qobj);
        else if (qel instanceof FuzzyQuery)
            process((FuzzyQuery) qel, qobj);
        else if (qel instanceof TermRangeQuery)
            process((TermRangeQuery) qel, qobj);
        else if (qel instanceof NumericRangeQuery)
            process((NumericRangeQuery) qel, qobj);
        else if (qel instanceof SpanQuery)
            process((SpanQuery) qel, qobj);
        else
            dbg("! Unknown Query Object");        
    }
    
    private void wireParentVertex(String label) throws Exception {
        Vertex newParentVertex = new Vertex(UUID.randomUUID().toString());
        newParentVertex.put("label", label);
        addEdge(newParentVertex, parentVertex, "resolve-to");
        parentVertex = newParentVertex;
    }
    
    private void process(BooleanQuery qel, JSONObject qobj) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        
        JSONObject _arg = newQueryObj(_OP_T);
        for(BooleanClause bcl : qel.getClauses()) {
            depth++;
            process(bcl, _arg);
            depth--;
        }
        getArgs(qobj).put(_arg);
    }
    
    private void process(BooleanClause qel, JSONObject qobj) throws Exception {
        String occurStr;
        
        if (qel.getOccur().equals(BooleanClause.Occur.MUST)) {
            occurStr = "MUST";
        } else if (qel.getOccur().equals(BooleanClause.Occur.MUST_NOT)) {
            occurStr = "MUST_NOT";
        } else if (qel.getOccur().equals(BooleanClause.Occur.SHOULD)) {
            occurStr = "SHOULD";
        } else {
            occurStr = "UNKNOWN";
        }
        
        dbg("(" + qel.getClass().getName() + ") " + qel);
        
        /*
        dbg("---- occur:      " + occurStr);
        dbg("---- required:   " + qel.isRequired());
        dbg("---- prohibited: " + qel.isProhibited());
        dbg("");
        */
        
        depth++;
        process(qel.getQuery(), qobj);
        depth--;
    }
    
    private void process(TermQuery qel, JSONObject qobj) throws Exception {
        Term term = qel.getTerm();
        
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("---- field: " + term.field());
        dbg("---- text:  " + term.text());
        dbg("");
        
        if (qobj.has(term.field())) {
            if (qobj.get(term.field()) instanceof JSONArray) {
                qobj.getJSONArray(term.field()).put(term.text());
            } else {
                String value0 = qobj.getString(term.field());
                qobj.remove(term.field());
                JSONArray values = new JSONArray();
                values.put(value0);
                values.put(term.text());
                qobj.put(term.field(), values);
            }
            //throw new Exception("cannot set field " + term.field() + " twice!");
        } else {
            qobj.put(term.field(), term.text());
        }
    }
    
    private void process(MultiTermQuery qel, JSONObject qobj) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(WildcardQuery qel, JSONObject qobj) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(PhraseQuery qel, JSONObject qobj) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
        
        String field = "_";
        String value = "";
        for(Term term : qel.getTerms()) {
            value += term.text() + " ";
            field = term.field();
        }
        value = value.trim();
        qobj.put(field, value);
    }
    
    private void process(PrefixQuery qel, JSONObject qobj) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(MultiPhraseQuery qel, JSONObject qobj) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(FuzzyQuery qel, JSONObject qobj) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(TermRangeQuery qel, JSONObject qobj) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");

        JSONObject _obj = newQueryObj(_RANGE_T);
        _obj.put("include_upper", qel.includesUpper());
        _obj.put("include_lower", qel.includesLower());
        _obj.put("lower", qel.getLowerTerm());
        _obj.put("upper", qel.getUpperTerm());
        _obj.put("field", qel.getField());
        getArgs(qobj).put(_obj);
    }
    
    private void process(NumericRangeQuery qel, JSONObject qobj) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(SpanQuery qel, JSONObject qobj) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }

}
