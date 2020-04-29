package iu.cadre.listeners.job;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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
}
