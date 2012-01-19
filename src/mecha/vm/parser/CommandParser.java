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
	private ListenableDirectedGraph<QueryNode, QueryEdge> queryGraph;
	
	public ListenableDirectedGraph<QueryNode, QueryEdge> parse(String query) throws Exception {
		return parse(DEFAULT_SEARCH_FIELD, query);
	}
	
	public ListenableDirectedGraph<QueryNode, QueryEdge> parse(String defaultField, String query) throws Exception {
		queryGraph = new ListenableDirectedGraph<QueryNode,QueryEdge>(QueryEdge.class);
		Analyzer analyzer = new WhitespaceAnalyzer();
		QueryParser luceneParser = new QueryParser(Version.LUCENE_35, defaultField, analyzer);
		luceneParser.setDefaultOperator(QueryParser.Operator.OR);
		luceneParser.setAllowLeadingWildcard(true);
		Query luceneQuery = luceneParser.parse(query);
		process(luceneQuery);

		if (SHOW_GRAPH) {
	       	FileWriter outFile = new FileWriter("query.dot");
	       	PrintWriter dot_wr = new PrintWriter(outFile);
	       	DOTExporter de = new DOTExporter(
	       	   new VertexNameProvider<QueryNode>() {
	       	       public String getVertexName(QueryNode v) {
	       	           try {
    	       	           return "" + v.this_node_id;
    	       	       } catch (Exception ex) {
    	       	           ex.printStackTrace();
    	       	           return null;
    	       	       }
	       	       }
	       	   },
	       	   new VertexNameProvider<QueryNode>() {
	       	       public String getVertexName(QueryNode v) {
	       	           return v.toString();
	       	       }
	       	   }, 
	       	   new EdgeNameProvider<QueryEdge>() {
	       	       public String getEdgeName(QueryEdge e) {
	       	           return e.toString();
	       	       }
	           },
	           new ComponentAttributeProvider<QueryNode>() {
	               public java.util.Map<java.lang.String,java.lang.String> getComponentAttributes(QueryNode v) {
	                   return new java.util.HashMap<java.lang.String,java.lang.String>();
	               }
	           },
	           new ComponentAttributeProvider<QueryEdge>() {
	               public java.util.Map<java.lang.String,java.lang.String> getComponentAttributes(QueryEdge e) {
	                   return new java.util.HashMap<java.lang.String,java.lang.String>();
	               }	               
	           }
            );
	       	de.export(dot_wr, queryGraph);
	       	outFile.close();
		}
		
		return queryGraph;
	}
	
	/*
	 * new!
	 *
    */
    
    private Deque<String> queryStack =
        new ArrayDeque<String>();
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
        //dbg("PROXYING: qel (" + qel.getClass().getName() + ") " + qel);
        
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
    
    
	

    /*
     * -----
     *  OLD
     * -----
    */

	private void interp(Query qq) throws Exception {
		interp(qq, 0);
	}
	
	private void interp(Query qq, int indent) throws Exception {
		QueryNode rootNode = new QueryNode(ref_uuid());
		rootNode.put("node_type", "result");
		rootNode.put("query_id", query_uuid());
		queryGraph.addVertex(rootNode);
		
		if (qq instanceof BooleanQuery) {
			interp(qq, indent, rootNode);
		} else {
			BooleanQuery bq = new BooleanQuery();
			bq.add(qq, BooleanClause.Occur.SHOULD);
			interp((Query)bq,indent,rootNode);
		}
	}
	
	private void interp(Query qq, int indent, QueryNode rootNode) throws Exception {
		QueryNode qn;
		QueryEdge<QueryNode> qe;
		String indentStr = "";
		
		for(int i=0; i<indent; i++) indentStr += "    ";
		BooleanQuery q = (BooleanQuery) qq;
		//dbg("Query = " + q.toString() + ", class = " + q.getClass());
		boolean root_is_setup = false;
		String prev_pred = null;
		for(BooleanClause bc : q.getClauses()) {
			BooleanClause.Occur bc_pred = bc.getOccur();
			String bc_pred_str;

			if (bc_pred == BooleanClause.Occur.MUST) {
				bc_pred_str = "AND";
			} else if (bc_pred == BooleanClause.Occur.MUST_NOT) {
				bc_pred_str = "AND_NOT";
			} else if (bc_pred == BooleanClause.Occur.SHOULD) {
				bc_pred_str = "OR";
			} else {
				bc_pred_str = "UNKNOWN? " + bc_pred.toString();
			}
			Query bc_q = bc.getQuery();

			if (root_is_setup == false || prev_pred != bc_pred_str) {
				qn = new QueryNode(ref_uuid());
				qn.put("node_type", "filter");
				qn.put("predicate", bc_pred_str);
				qn.put("info", bc.toString());
				queryGraph.addVertex(qn);
				
				String n_lbl;
				if (bc_pred_str == "AND_NOT") n_lbl = "exclusion-filter";
				else n_lbl = "has-argument";
				
				if (n_lbl == "exclusion-filter") {
					qe = new QueryEdge<QueryNode>(qn, rootNode, n_lbl);
					qe.put("rel", n_lbl);
					queryGraph.addEdge(qn, rootNode, qe);
				}
				
				if (bc_pred_str != "AND_NOT" && qn != null && rootNode != null) {
					//dbg("/// qn = " + qn.toString());
					//dbg("/// rn = " + rootNode.toString());
					QueryEdge<QueryNode> qe2 = new QueryEdge<QueryNode>(qn, rootNode, "emit");
					qe2.put("rel", "emit");
					queryGraph.addEdge(qn, rootNode, qe2);
				}
				root_is_setup = true;
				rootNode = qn;
				prev_pred = bc_pred_str;
			}
			qn = new QueryNode(ref_uuid());
			//dbg(indentStr + "bc = " + bc + ", bc_q = " + bc_q + ", bc_q.class = " + bc_q.getClass());
			if (bc_q instanceof TermQuery) {
				TermQuery tq = (TermQuery) bc_q;
				//dbg("bc_q: TermQuery: term = " + tq.getTerm());
				qn.put("query", tq);
				qn.put("field", tq.getTerm().field());
				qn.put("term", tq.getTerm().text());
				qn.put("node_type", "term_query");
				queryGraph.addVertex(qn);
				
			} else if (bc_q instanceof BooleanQuery) {
				BooleanQuery bq = (BooleanQuery) bc_q;
				interp(bq,indent+1, rootNode);
				
			} else if (bc_q instanceof TermRangeQuery) {
				TermRangeQuery trq = (TermRangeQuery) bc_q;
				//dbg("bc_q: TermRangeQuery: term = " + trq.getTerm());
				qn.put("query", trq);
				qn.put("node_type", "get_range");
				qn.put("field", trq.getField());
				qn.put("lower_key", trq.getLowerTerm());
				qn.put("upper_key", trq.getUpperTerm());
				qn.put("lower_inclusive", trq.includesLower());
				qn.put("upper_inclusive", trq.includesUpper());
				queryGraph.addVertex(qn);
				
			} else if (bc_q instanceof FuzzyQuery) {
				FuzzyQuery pq = (FuzzyQuery) bc_q;
				//dbg("bc_q: FuzzyQuery: term = " + pq.getTerm());
				qn.put("query", pq);
				qn.put("node_type", "fuzzy_query");
				qn.put("field", pq.getTerm().field());
				qn.put("term", pq.getTerm().text());
				qn.put("min_similarity", pq.getMinSimilarity());
				queryGraph.addVertex(qn);
				
			} else if (bc_q instanceof WildcardQuery) {
				WildcardQuery pq = (WildcardQuery) bc_q;
				//dbg("bc_q: WildcardQuery: term = " + pq.getTerm());
				qn.put("query", pq);
				qn.put("node_type", "wildcard_query");
				qn.put("field", pq.getTerm().field());
				qn.put("wildcard_term", pq.getTerm());
				queryGraph.addVertex(qn);

			} else if (bc_q instanceof PhraseQuery) {
				PhraseQuery pq = (PhraseQuery) bc_q;
				//dbg("bc_q: PhraseQuery: term = " + pq.getTerms());
				qn.put("query", pq);
				qn.put("node_type", "phrase_query");
				qn.put("terms", pq.getTerms());
				queryGraph.addVertex(qn);
				
			} else if (bc_q instanceof PrefixQuery) {
				PrefixQuery pq = (PrefixQuery) bc_q;
				//dbg("bc_q: PrefixQuery: term = " + pq.getPrefix());
				qn.put("query", pq);
				qn.put("node_type", "prefix_query");
				qn.put("field", pq.getPrefix().field());
				qn.put("prefix", pq.getPrefix());
				queryGraph.addVertex(qn);
				
			} else if (bc_q instanceof NumericRangeQuery) {
				NumericRangeQuery trq = (NumericRangeQuery) bc_q;
				//dbg("bc_q: NumericRangeQuery: term = " + trq.getTerm());
				qn.put("query", trq);
				qn.put("node_type", "get_num_range");
				qn.put("field", trq.getField());
				qn.put("min", trq.getMin());
				qn.put("max", trq.getMax());
				qn.put("min_inclusive", trq.includesMin());
				qn.put("max_inclusive", trq.includesMax());
				queryGraph.addVertex(qn);
				
			} else {
				qn.put("node_type", "unknown");
				qn.put("nodeClassName", bc_q.getClass().toString());
				qn.put("object", bc_q);
				qn.put("predicate", bc_pred_str);
				queryGraph.addVertex(qn);
				
				qe = new QueryEdge<QueryNode>(rootNode, qn, "has-argument");
				qe.put("rel", "has-argument");
				qe.put("predicate", bc_pred_str);
				//dbg("adding edge rootNode = " + rootNode + ", \nqn = " + qn + ", " + qe.toString(4));
				queryGraph.addEdge(rootNode, qn, qe);
				dbg("? " + bc_q.getClass());
			}
			
			if (qn != null && rootNode != null) {
				if (bc_q instanceof BooleanQuery) {
				} else {
					//dbg("/// qn = " + qn.toString());
					//dbg("/// rn = " + rootNode.toString());
					QueryEdge<QueryNode> qe2 = new QueryEdge<QueryNode>(qn, rootNode, "emit");
					qe2.put("rel", "emit");
					queryGraph.addEdge(qn, rootNode, qe2);
				}
			}
		}
		JSONObject q_json = new JSONObject();
	}
	
	public static String ref_uuid() {
        return query_uuid("ref");
	}
	
	public static String query_uuid() {
		return query_uuid("query");
	}
	
	public static String query_uuid(String s) {
		return s + "," + UUID.randomUUID().toString();
		/* + "," +
            System.getenv("RIAK_NODENAME") + "@" +
            System.getenv("RIAK_MACHINE_IP");
        */
	}
	
	// soon to be deprecated
    public void plan() throws Exception {
		dbg("--------------------------------------------------");
		Set<QueryNode> nodes = queryGraph.vertexSet();
		
		ArrayList<QueryNode> startNodes = 
            new ArrayList<QueryNode>();
        ArrayList<QueryNode> workerNodes =
            new ArrayList<QueryNode>();
        QueryNode resultNode = null;
		
		for(QueryNode n: nodes) {
            Set<QueryEdge> edges_in = queryGraph.incomingEdgesOf(n);
            Set<QueryEdge> edges_out = queryGraph.outgoingEdgesOf(n);
            //dbg("incoming edges: " + edges_in.toString());
            //dbg("outgoing edges: " + edges_in.toString());
            
            if (edges_in.size() == 0) {
                //dbg("start node, n = " + n.toString(4));
                startNodes.add(n);
            } else if (edges_in.size() > 0 &&
                edges_out.size() > 0) {
                workerNodes.add(n);
            } else if (edges_out.size() == 0) {
                resultNode = n;
            }
            
            dbg("--------------------------------------------------");
        }
        
        dbg("\n\n");
        
        dbg("startNodes: " + startNodes);
        dbg("workerNodes: " + workerNodes);
        dbg("resultNode: " + resultNode);
	}
	
    public static void main(String args[]) throws Exception {
		String q;
		if (args.length > 0) {
			q = args[0];
		} else {
			q = "product:camera +mpix:30 -film";
		}
		//dbg("q = " + q);
		CommandParser lp = new CommandParser();
		DirectedGraph<QueryNode, QueryEdge> riakQuery = lp.parse(q);
		lp.plan();
    }
    
    /*
    public static void main(String[] args) throws Exception {
    
        String q = "";
        for(int i=0; i<args.length; i++) {
            q += args[i] + " ";
        }
        q = q.trim();
        
        dbg("q = '" + q + "'");
        
    }
    */

}
