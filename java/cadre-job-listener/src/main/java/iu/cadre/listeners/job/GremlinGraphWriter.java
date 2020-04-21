package iu.cadre.listeners.job;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GremlinGraphWriter {
    public static void main(String[] args) {
        try {
            TinkerIoRegistryV3d0 v = TinkerIoRegistryV3d0.instance();

            GraphTraversalSource g = EmptyGraph.instance().traversal().withRemote("conf/remote-graph.properties");

            Object paperTitle = "big data technologies a survey";
            Map node = g.V().has("paperTitle", paperTitle).valueMap().next();

            System.out.printf("The paper '%s' has %s citations.\n", node.get("originalTitle"), node.get("citationCount"));

            TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden").inE("AuthorOf").subgraph("sg").cap("sg").next();

            sg.io(IoCore.graphml()).writeGraph("result.graphml");

            PrintStream ps = new PrintStream(System.out);
            dump_graph(sg, ps);
            g.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void dump_graph(TinkerGraph sg, OutputStream stream)
    {
        PrintStream ps = new PrintStream(stream);

        for (Iterator<Vertex> it = sg.vertices(); it.hasNext(); ) {
            Vertex v = it.next();
            ps.printf("Vertex %s: ", v.label());
            for (Iterator<VertexProperty<Object>> it2 = v.properties(); it2.hasNext(); ) {
                VertexProperty<Object> p = it2.next();
                ps.printf("%s = %s;", p.key(), p.value());
            }
            ps.printf("id: %s\n", v.id());
        }

        for (Iterator<Edge> it = sg.edges(); it.hasNext(); ) {
            Edge e = it.next();
            ps.printf("Edge %s: ", e.label());
            for (Iterator<Property<Object>> it2 = e.properties(); it2.hasNext(); ) {
                Property<Object> p = it2.next();
                ps.printf("%s = %s;", p.key(), p.value());
            }
            ps.printf("id: %s [%s -> %s]\n", e.id(), e.inVertex().id(), e.outVertex().id());
        }
    }

    public static void graph_to_ml(TinkerGraph sg, OutputStream stream) throws IOException {
        GraphMLWriter.build().create().writeGraph(stream, sg);
    }

    public static List<String> gather_property_names(TinkerGraph sg)
    {
        List<String> result = Stream.of(new String[]{}).collect(Collectors.toList());
        for (Iterator<Vertex> it = sg.vertices(); it.hasNext(); ) {
            Vertex v = it.next();
            for (Iterator<VertexProperty<Object>> it2 = v.properties(); it2.hasNext(); ) {
                VertexProperty<Object> p = it2.next();
                result.add(p.key());
            }
        }

        for (Iterator<Edge> it = sg.edges(); it.hasNext(); ) {
            Edge e = it.next();
            for (Iterator<Property<Object>> it2 = e.properties(); it2.hasNext(); ) {
                Property<Object> p = it2.next();
                result.add(p.key());
            }
        }
        return result;
    }

    public static void graph_to_csv(TinkerGraph sg, OutputStream stream) throws IOException {
        PrintStream ps = new PrintStream(stream);

        ArrayList<String> base_headers = new ArrayList<String>(Arrays.asList("Type", "ID", "Label", "InVertex", "OutVertex"));
        List<String> properties = gather_property_names(sg);
        List<String> header = ListUtils.union(base_headers, properties);
        CSVPrinter csvPrinter = new CSVPrinter(ps, CSVFormat.DEFAULT.withHeader(header.stream()
                .toArray(String[]::new)));

        for (Iterator<Vertex> it = sg.vertices(); it.hasNext(); ) {
            Vertex v = it.next();
            List<String> base_values = new ArrayList<String>(Arrays.asList("Vertex",
                    v.id().toString(), v.label(), "", ""));
            List<String> values = ListUtils.union(base_values, gather_property_values(properties, v));
            csvPrinter.printRecord(values);
        }

        for (Iterator<Edge> it = sg.edges(); it.hasNext(); ) {
            Edge e = it.next();
            List<String> base_values = new ArrayList<String>(Arrays.asList("Edge",
                    e.id().toString(), e.label(), e.inVertex().id().toString(), e.outVertex().id().toString()));
            List<String> values = ListUtils.union(base_values, gather_property_values(properties, e));
            csvPrinter.printRecord(values);
        }
        csvPrinter.flush();
    }

    private static List<String> gather_property_values(List<String> properties, Element v) {
        ArrayList<String> result = new ArrayList<String>();
        for(String p : properties){
            Property<Object> x = v.property(p);
            if (x.isPresent())
                result.add(x.value().toString());
            else
                result.add("");
        }
        return result;
    }
}
