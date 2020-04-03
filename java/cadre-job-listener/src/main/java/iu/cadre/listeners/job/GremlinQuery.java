package iu.cadre.listeners.job;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.util.Map;

public class GremlinQuery {
    public static void main(String[] args) {
        try {
            TinkerIoRegistryV3d0 v = TinkerIoRegistryV3d0.instance();

            GraphTraversalSource g = EmptyGraph.instance().traversal().withRemote("conf/remote-graph.properties");

            Object paperTitle = "big data technologies a survey";
            Map node = g.V().has("paperTitle", paperTitle).valueMap().next();

            System.out.printf("The paper '%s' has %s citations.\n", node.get("originalTitle"), node.get("citationCount"));

            TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden").inE("AuthorOf").subgraph("sg").cap("sg").next();

            sg.io(IoCore.graphml()).writeGraph("result.graphml");

            g.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
