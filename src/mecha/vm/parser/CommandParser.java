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
	private static String DEFAULT_SEARCH_FIELD = "default";
	private ListenableDirectedGraph<Vertex, Edge> queryGraph;
	
	public ListenableDirectedGraph<Vertex, Edge> parse(String query) throws Exception {
		return parse(DEFAULT_SEARCH_FIELD, query);
	}
	
	public ListenableDirectedGraph<Vertex, Edge> 
	   parse(String defaultField, String query) throws Exception {
		queryGraph = new ListenableDirectedGraph<Vertex,Edge>(Edge.class);
		Analyzer analyzer = new WhitespaceAnalyzer();
		QueryParser luceneParser = new QueryParser(Version.LUCENE_35, defaultField, analyzer);
		luceneParser.setDefaultOperator(QueryParser.Operator.OR);
		luceneParser.setAllowLeadingWildcard(true);
		Query luceneQuery = luceneParser.parse(query);
		process(luceneQuery);

		if (SHOW_GRAPH) {
	       	FileWriter outFile = new FileWriter("./tmp/query.dot");
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
    
    //
    // INFO: query (org.apache.lucene.search.BooleanQuery) $:cmd (param1:value1 param2:value2)
    //
    private void process(Query qel) throws Exception {
        if (qel instanceof BooleanQuery)
            process((BooleanQuery) qel);
        else if (qel instanceof TermQuery)
            process((TermQuery) qel);
        else if (qel instanceof MultiTermQuery)
            process((MultiTermQuery) qel);
        else if (qel instanceof WildcardQuery)
            process((WildcardQuery) qel);
        else if (qel instanceof PhraseQuery)
            process((PhraseQuery) qel);
        else if (qel instanceof PrefixQuery)
            process((PrefixQuery) qel);
        else if (qel instanceof MultiPhraseQuery)
            process((MultiPhraseQuery) qel);
        else if (qel instanceof FuzzyQuery)
            process((FuzzyQuery) qel);
        else if (qel instanceof TermRangeQuery)
            process((TermRangeQuery) qel);
        else if (qel instanceof NumericRangeQuery)
            process((NumericRangeQuery) qel);
        else if (qel instanceof SpanQuery)
            process((SpanQuery) qel);
        else
            dbg("! Unknown Query Object");        
    }
    
    private void process(BooleanQuery qel) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        for(BooleanClause bcl : qel.getClauses()) {
            depth++;
            process(bcl);
            depth--;
        }
    }
    
    private void process(BooleanClause qel) throws Exception {
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
        process(qel.getQuery());
        depth--;
    }
    
    private void process(TermQuery qel) throws Exception {
        Term term = qel.getTerm();
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("---- field: " + term.field());
        dbg("---- text:  " + term.text());
        dbg("");
    }
    
    private void process(MultiTermQuery qel) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(WildcardQuery qel) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(PhraseQuery qel) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(PrefixQuery qel) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(MultiPhraseQuery qel) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(FuzzyQuery qel) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(TermRangeQuery qel) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(NumericRangeQuery qel) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }
    
    private void process(SpanQuery qel) throws Exception {
        dbg("(" + qel.getClass().getName() + ") " + qel);
        dbg("");
    }

}
