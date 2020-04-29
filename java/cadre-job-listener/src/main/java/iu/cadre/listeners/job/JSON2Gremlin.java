package iu.cadre.listeners.job;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.janusgraph.core.attribute.Text.textContainsFuzzy;

public class JSON2Gremlin {

    public static TinkerGraph load_graph(GraphTraversalSource g, JSONObject q) throws UnsupportedOperationException, JSONException {
        if (q.get("dataset") != "mag")
            throw new UnsupportedOperationException("Only MAG database is supported");

        JSONArray graph = (JSONArray) q.get("graph");
        Object o = g.V();
        for (int i = 0; i<graph.length(); ++i )
        {
            String vertex_type = (String) ((JSONObject)graph.get(i)).get("vertexType");
            o = g.V().hasLabel(vertex_type);
        }
        return (TinkerGraph) ((GraphTraversal) o).bothE().subgraph("sg").cap("sg").next();
    }

    public static GraphTraversalSource getSubGraphForQuery(GraphTraversalSource traversal, JsonObject graphFields, List<String> outputFields){
        GraphTraversal<Vertex, Vertex> filterTraversal = traversal.V();
        JsonArray nodes = graphFields.get("nodes").getAsJsonArray();
        JsonArray edges = graphFields.get("edges").getAsJsonArray();
        Map<String, List<Object>> asLabelFilters = getASLabelFilters(nodes);
        int count = 1;
        List<Object> allMatchClauses = new ArrayList<>();
        for (String vertexType : asLabelFilters.keySet()){
            String label1 = "label" + count;
            count++;
            String label2 = "label" + count;
            List<Object> hasFilterListPerVertex = asLabelFilters.get(vertexType);
            allMatchClauses.addAll(hasFilterListPerVertex);

            for (int i=0; i < edges.size(); i++){
                JsonObject edgeJson = edges.get(i).getAsJsonObject();
                String sourceVertex = edgeJson.get("source").getAsString();
                String targetVertex = edgeJson.get("target").getAsString();
                String relation = edgeJson.get("relation").getAsString();
                if (sourceVertex.equals("paper") && targetVertex.equals("journal")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(relation).subgraph("sg1").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (sourceVertex.equals("paper") && targetVertex.equals("conferenceInstance")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(relation).subgraph("sg1").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (sourceVertex.equals("journal") && targetVertex.equals("paper")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(relation).subgraph("sg1").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (sourceVertex.equals("author") && targetVertex.equals("paper")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).outE(relation).subgraph("sg1").inV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (sourceVertex.equals("conferenceInstance") && targetVertex.equals("paper")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(relation).subgraph("sg1").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }else if (sourceVertex.equals("paper") && targetVertex.equals("author")){
                    GraphTraversal<Object, Vertex> nextAsLabel = __.as(label1).inE(relation).subgraph("sg1").outV().as(label2);
                    allMatchClauses.add(nextAsLabel);
                }
            }
        }
        TinkerGraph tg = (TinkerGraph)filterTraversal.match((Traversal<?, ?>) allMatchClauses).cap("sg").next();
        GraphTraversalSource sg = tg.traversal();
        return sg;
    }

    public static Map<String, List<Object>> getASLabelFilters(JsonArray nodes){
        Map<String, List<Object>> hasFilterMap = new LinkedHashMap<>();
        for (int i=0; i < nodes.size(); i++){
            String label = "label" + (i+1);
            JsonObject vertexObject = nodes.get(i).getAsJsonObject();
            String vertexType = vertexObject.get("vertexType").getAsString();
            JsonArray filters = vertexObject.get("filters").getAsJsonArray();
            List< Object> hasFilters = new ArrayList<>();
            for (int j = 0; j < filters.size(); j++) {
                JsonObject filterField = filters.get(j).getAsJsonObject();
                String field = filterField.get("field").getAsString();
                String value = filterField.get("value").getAsString();
                String[] fieldValues = new String[]{field, value};
                String operator = filterField.get("operator").getAsString();
                if (!field.equals("year") && !field.equals("doi")){
                    GraphTraversal<Object, Object> asLabelWithFilters = __.as(label).has(vertexType, field, textContainsFuzzy(value));
                    hasFilters.add(asLabelWithFilters);
                }else {
                    GraphTraversal<Object, Object> asLabelWithFilters = __.as(label).has(vertexType, field, Integer.valueOf(value));
                    hasFilters.add(asLabelWithFilters);
                }
            }
            hasFilterMap.put(vertexType, hasFilters);
        }
        return hasFilterMap;
    }


}
