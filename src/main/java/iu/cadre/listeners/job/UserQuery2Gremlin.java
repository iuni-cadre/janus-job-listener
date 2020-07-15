package iu.cadre.listeners.job;

import iu.cadre.listeners.job.util.ConfigReader;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.janusgraph.core.attribute.Text.textContains;

public class UserQuery2Gremlin {
    private static final Logger LOG = LoggerFactory.getLogger(UserQuery2Gremlin.class);

    public static TinkerGraph getSubGraphForQuery(GraphTraversalSource traversal, UserQuery query) throws Exception {
        if (!query.DataSet().equals("mag"))
            throw new UnsupportedOperationException("Only MAG database is supported");

        LOG.info("Creating subgraph for the query");
        GraphTraversal<Vertex, Vertex> filterTraversal = traversal.V();

        List<Edge> edges = query.Edges();
        if (edges.isEmpty())
        {
            Edge e = new Edge();
            e.source = "Paper";
            e.target = "Author";
            e.relation = "AuthorOf";
            edges.add(e);
        }
        Map<String, List<Object>> asLabelFilters = getASLabelFilters(query.Nodes());
        int count = 1;
        List<Object> allMatchClauses = new ArrayList<>();
        for (String vertexType : asLabelFilters.keySet()){
            String label1 = "label" + count;
            count++;
            String label2 = "label" + count;
            List<Object> hasFilterListPerVertex = asLabelFilters.get(vertexType);
            allMatchClauses.addAll(hasFilterListPerVertex);

            for (Edge edge : edges) {
                LOG.info(edge.toString());
                if (edge.source.equals("Paper") && edge.target.equals("JournalFixed")) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals("Paper") && edge.target.equals("ConferenceInstance")) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals("JournalFixed") && edge.target.equals("Paper")) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals("Author") && edge.target.equals("Paper")) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(edge.relation).subgraph("sg").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals("ConferenceInstance") && edge.target.equals("Paper")) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals("Paper") && edge.target.equals("Author")) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                } else if (edge.source.equals("Paper") && edge.target.equals("Paper")) {
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(edge.relation).subgraph("sg").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }
            }
        }

        GraphTraversal<?, ?>[] temp = new GraphTraversal[allMatchClauses.size()];
        for (int i = 0; i < allMatchClauses.size(); i++) {
            temp[i] = (GraphTraversal<?, ?>) allMatchClauses.get(i);
        }

        LOG.info(allMatchClauses.size() + " total clauses in query");

        TinkerGraph tg = (TinkerGraph)filterTraversal.match(temp).limit(ConfigReader.getJanusRecordLimit()).cap("sg").next();

        return tg;
    }

    public static Map<String, List<Object>> getASLabelFilters(List<Node> nodes){
        Map<String, List<Object>> hasFilterMap = new LinkedHashMap<>();
        int i = 0;
        for (Node node: nodes)
        {
            String label = "label" + (i+1);
            if (node.filters.isEmpty())
            {
                LOG.warn("Node without filters requested, rejecting");
                continue;
            }
            String vertexType = node.type;
            List<Object> hasFilters = new ArrayList<>();
            for (Filter filterField : node.filters) {
                LOG.info(filterField.field + " = " + filterField.value);
                if (!filterField.field.equals("year") && !filterField.field.equals("doi")) {
                    GraphTraversal<Object, Object> asLabelWithFilters = __.as(label).has(vertexType, filterField.field, textContains(filterField.value));
                    hasFilters.add(asLabelWithFilters);
                } else {
                    GraphTraversal<Object, Object> asLabelWithFilters = __.as(label).has(vertexType, filterField.field, Integer.valueOf(filterField.value));
                    hasFilters.add(asLabelWithFilters);
                }

                if (filterField.operator.equals("or")) {
                    int n = hasFilters.size() - 1;
                    GraphTraversal gt1 = (GraphTraversal<Object, Object>) hasFilters.get(n);
                    GraphTraversal gt2 = (GraphTraversal<Object, Object>) hasFilters.get(n - 1);
                    hasFilters.remove(gt1);
                    hasFilters.remove(gt2);
                    hasFilters.add(__.as(label).or(gt1, gt2));
                }
            }
            hasFilterMap.put(vertexType, hasFilters);
            i++;
        }
        return hasFilterMap;
    }


}
