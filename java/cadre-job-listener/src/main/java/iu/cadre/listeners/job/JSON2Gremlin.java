package iu.cadre.listeners.job;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.json.simple.JSONObject;

public class JSON2Gremlin {

    public static TinkerGraph load_graph(GraphTraversalSource g, JSONObject q)
    {
        return (TinkerGraph)g.V().has("name", "Kelvin").subgraph("sg").cap("sg").next();
    }
}
