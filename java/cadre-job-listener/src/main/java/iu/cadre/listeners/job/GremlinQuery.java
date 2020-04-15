package iu.cadre.listeners.job;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.util.Iterator;
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

            dump_graph(sg);
            g.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void dump_graph(TinkerGraph sg)
    {
        System.out.printf("Writing %s\n", sg.toString());
        for (Iterator<Edge> it = sg.edges(); it.hasNext(); ) {
            Edge e = it.next();
            System.out.printf("Edge: ");
            for (Iterator<Property<Object>> it2 = e.properties(); it2.hasNext(); ) {
                Property<Object> p = it2.next();
                System.out.printf("%s = %s;", p.key(), p.value());
            }
            System.out.printf("\n");
        }

        for (Iterator<Vertex> it = sg.vertices(); it.hasNext(); ) {
            Vertex v = it.next();
            System.out.printf("Vertex %s: ", v.label());
            for (Iterator<VertexProperty<Object>> it2 = v.properties(); it2.hasNext(); ) {
                VertexProperty<Object> p = it2.next();
                System.out.printf("%s = %s;", p.key(), p.value());
            }
            System.out.printf("\n");
        }

    }

}
