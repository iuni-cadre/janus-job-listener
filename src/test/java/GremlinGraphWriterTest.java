import com.google.common.collect.Iterators;
import iu.cadre.listeners.job.GremlinGraphWriter;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.janusgraph.core.attribute.Text.textContainsFuzzy;

class GremlinGraphWriterTest {

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
    void JanusGraph_can_be_built() {
        JanusGraph graph = JanusGraphFactory.open("inmemory");
        GraphTraversalSource g = graph.traversal();
        g.addV("person").property("name","Kelvin").iterate();
        Vertex v = g.V().has("name", "Kelvin").next();
        assertNotNull(v.id());
    }

    @Test
    void can_query_for_paper()
    {
        TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden")
                .inE("AuthorOf").subgraph("sg").cap("sg").next();
        assertEquals(2, Iterators.size(sg.vertices()));
        assertEquals(1, Iterators.size(sg.edges()));
    }

    @Test
    void can_query_for_journal()
    {
        TinkerGraph sg = (TinkerGraph)g.V().has("JournalRev", "normalizedName", "the open acoustics journal")
                .inE().subgraph("sg").cap("sg").next();
        assertEquals(7, Iterators.size(sg.vertices()));
        assertEquals(6, Iterators.size(sg.edges()));
    }

    @Test
    void all_papers_for_year_limit_ten()
    {
        TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "year", "1900").limit(10).inE("AuthorOf")
                .subgraph("sg").cap("sg").next();
        assertEquals(10, Iterators.size(sg.edges()));
    }

    @Test
    void all_papers_between_two_years_limit_ten()
    {
        TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "year", P.between(1945f, 1946f)).limit(10)
                .inE("AuthorOf")
                .subgraph("sg").cap("sg").next();

        assertEquals(10, Iterators.size(sg.edges()));
    }

    @Test
    void all_papers_from_journal_for_year()
    {
        TinkerGraph sg = (TinkerGraph)g.V().has("JournalRev","normalizedName", "the open acoustics journal")
                .inE("PublishedInRev").subgraph("sg")
                .outV()
                .has("Paper", "year", "2001").cap("sg").next();


        assertEquals(6, Iterators.size(sg.vertices()));
    }

    @Test
    void can_write_to_graphml()
    {
        try {
            TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden")
                    .inE("AuthorOf").subgraph("sg").cap("sg").next();
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            GremlinGraphWriter.graph_to_ml(sg, stream);
            String actualResult = stream.toString();
            assertTrue(actualResult.contains("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\""));
            assertTrue(actualResult.contains("<data key=\"labelV\">Author</data><data key=\"name\">joe</data>"));
            assertTrue(actualResult.contains("<data key=\"labelV\">Paper</data><data key=\"paperTitle\">full case study report upplandsbondens sweden</data>"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    void can_write_to_stream()
    {
        TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden")
                .inE("AuthorOf").subgraph("sg").cap("sg").next();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        GremlinGraphWriter.dump_graph(sg, stream);
        String actualResult = stream.toString();
        assertTrue(actualResult.contains("Vertex Author: name = joe;id:"));
        assertTrue(actualResult.contains("Vertex Paper: paperTitle = full case study report upplandsbondens sweden;id:"));
        assertTrue(actualResult.contains("Edge AuthorOf: test_edge_property = test_edge_property_value;id:"));
    }

    @Test
    void can_write_to_csv()
    {
        try {
            TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden")
                    .inE("AuthorOf").subgraph("sg").cap("sg").next();
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            GremlinGraphWriter.graph_to_csv(sg, stream);
            String actualResult = stream.toString();

            String[] splits = actualResult.split("\n");
            String header = splits[0];
            assertTrue(header.contains("Type,ID,Label,InVertex,OutVertex"));
            assertTrue(header.contains("name,paperTitle,test_edge_property"));

            // jumping through some hoops here to avoid dealing with the ID's in the output
            assertTrue(actualResult.contains("Paper,,,,full case study report upplandsbondens sweden,"));
            assertTrue(actualResult.contains("Author,,,joe,,"));
            assertTrue(actualResult.contains("AuthorOf"));
            assertTrue(actualResult.contains(",,,test_edge_property_value"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void gather_property_names()
    {
        TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden")
                .inE("AuthorOf").subgraph("sg").cap("sg").next();
        List<String> actual = GremlinGraphWriter.gather_property_names(sg);
        assertEquals(3, actual.size());
        assertTrue(actual.contains("paperTitle"));
        assertTrue(actual.contains("name"));
        assertTrue(actual.contains("test_edge_property"));
    }
}
