import com.google.common.collect.Iterators;
import iu.cadre.listeners.job.JSON2Gremlin;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;
import static org.junit.jupiter.api.Assertions.*;

import org.json.simple.JSONObject;

public class JSON2GremlinTest {
    static GraphTraversalSource g;

    @BeforeAll
    static void build_graph()
    {
        JanusGraph graph = JanusGraphFactory.build().set(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory")
                .set(TIMESTAMP_PROVIDER.toStringWithoutRoot(), "NANO").open();
        g = graph.traversal();

        g.addV("Paper").property("paperTitle","full case study report upplandsbondens sweden").as("v1").
                addV("Author").property("name","joe").as("v2").
                addE("AuthorOf").property("test_edge_property", "test_edge_property_value").from("v2").to("v1").iterate();

        g.addV("JournalRev").property("normalizedName","the open acoustics journal").as("v1").
                addV("Paper").property("name","coolpaper").as("v2").
                addE("PublishedIn").from("v2").to("v1").iterate();

        for (int i = 0; i<20; ++i)
            g.addV("Paper").property("paperTitle",String.format("Paper%s", i)).
                    property("year", "1900").as("v1").
                    addV("Author").property("name","joe").as("v2").
                    addE("AuthorOf").from("v2").to("v1").iterate();

        for (int i = 0; i<20; ++i)
            g.addV("Paper").property("paperTitle",String.format("Paper%s", i+1945)).
                    property("year", 1945f).as("v1").
                    addV("Author").property("name","jane").as("v2").
                    addE("AuthorOf").from("v2").to("v1").iterate();

        Vertex journal = g.V().hasLabel("JournalRev").next();
        for (int i = 0; i<5; ++i)
            g.addV("Paper").property("paperTitle",String.format("Paper%s", i+2001)).
                    property("year", 2001).as("v1").
                    addE("PublishedInRev").from("v1").to(journal).iterate();
        g.tx().commit();
    }

    @Test
    void can_create_graph_from_json() {
        JSONParser parser = new JSONParser();

        //String s = "{filters: [{\"field\": \"year\", \"field\": \"1990\", \"operation\": \"AND\"}, {\"field\": \"paper_title\", \"value\": \"unicorns\", \"operation\": \"\"}]}";
        String s = "{\"filters\": [{\"field\": \"year\", \"field\": \"1990\", \"operation\": \"AND\"}, {\"field\": \"paper_title\", \"value\": \"unicorns\", \"operation\": \"\"}]}";
        JSONObject  obj = null;
        try {
            obj = (JSONObject)parser.parse(s);
            TinkerGraph tg = JSON2Gremlin.load_graph(g, obj);
            assertNotNull(tg);
            System.out.println(obj.get("filters"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
