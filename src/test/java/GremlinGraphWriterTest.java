import com.google.common.collect.Iterators;
import iu.cadre.listeners.job.GremlinGraphWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

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

        g.addV("Paper").property("paperTitle","full case study report upplandsbondens sweden")
                             .as("v1").
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
            List<Map<String, String>> csvInputList = parse_csv(stream);
            assertEquals(3, csvInputList.size());

            Map<String, String> author = csvInputList.stream()
                    .filter(value -> value.get("Label").equals("Author"))
                    .findFirst().get();
            assertEquals("joe", author.get("name"));

            Map<String, String> paper = csvInputList.stream()
                    .filter(value -> value.get("Label").equals("Paper"))
                    .findFirst().get();
            assertEquals("full case study report upplandsbondens sweden", paper.get("paperTitle"));

            Map<String, String> authorOf = csvInputList.stream()
                    .filter(value -> value.get("Label").equals("AuthorOf"))
                    .findFirst().get();
            assertEquals(paper.get("ID"), authorOf.get("InVertex"));
            assertEquals(author.get("ID"), authorOf.get("OutVertex"));

            /*
            String[] splits = actualResult.split("\n");
            Set<String> header = new HashSet<>(Arrays.asList(splits[0].trim().split(",")));
            Set<String> expected = new HashSet<>(Arrays.asList("Type", "ID", "Label", "InVertex", "OutVertex",
                    "name","paperTitle","test_edge_property"));
            assertEquals(expected, header);

            // jumping through some hoops here to avoid dealing with the ID's in the output
            assertTrue(actualResult.contains("Paper,,,,,full case study report upplandsbondens sweden,"));
            assertTrue(actualResult.contains("Author,,,joe,,"));
            assertTrue(actualResult.contains("AuthorOf"));
            assertTrue(actualResult.contains(",,,test_edge_property_value"));
            */
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    void gather_property_names()
    {
        TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "paperTitle", "full case study report upplandsbondens sweden")
                .inE("AuthorOf").subgraph("sg").cap("sg").next();
        List actual = GremlinGraphWriter.gather_property_names(sg);
        assertEquals(3, actual.size());
        assertTrue(actual.contains("paperTitle"));
        assertTrue(actual.contains("name"));
        assertTrue(actual.contains("test_edge_property"));
    }

    @Test
    void gather_property_names__returns_no_duplicates()
    {
        TinkerGraph sg = (TinkerGraph)g.V().has("Paper", "year", "1900")
                .inE("AuthorOf").subgraph("sg").cap("sg").next();
        List actual = GremlinGraphWriter.gather_property_names(sg);
        assertEquals("tinkergraph[vertices:40 edges:20]", sg.toString());
        assertEquals(3, actual.size());
        assertTrue(actual.contains("paperTitle"));
        assertTrue(actual.contains("name"));
        assertTrue(actual.contains("year"));

    }

    @Test
    void projection_to_csv__writes_nothing_on_list_empty()
    {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            GremlinGraphWriter.projection_to_csv(new ArrayList<Map>(), stream);
            assertEquals( "", stream.toString());
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    void projection_to_csv()
    {
        Map<String, String> map = new HashMap<>();

        map.put("title", "My Paper"); // put example
        map.put("author", "betty");
        map.put("journal", "Science");

        List<Map> p = Arrays.asList(map);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            GremlinGraphWriter.projection_to_csv(p, stream);
            List<Map<String, String>> csvInputList = parse_csv(stream);
            assertEquals(1, csvInputList.size());
            assertEquals(map, csvInputList.get(0));
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    private List<Map<String, String>> parse_csv(ByteArrayOutputStream stream) throws IOException {
        Reader in = new StringReader(stream.toString());
        CSVFormat format = CSVFormat.newFormat(',').withHeader();
        CSVParser parser = new CSVParser(in,format);
        List<CSVRecord> actualResult = parser.getRecords();
        List<Map<String, String>> csvInputList = new CopyOnWriteArrayList<>();
        List<Map<String, Integer>> headerList = new CopyOnWriteArrayList<>();
        Map<String, Integer> headerMap = parser.getHeaderMap();
        for(CSVRecord record : actualResult){
            Map<String, String> inputMap = new LinkedHashMap<>();

            for(Map.Entry<String, Integer> header : headerMap.entrySet()){
                inputMap.put(header.getKey(), record.get(header.getValue()));
            }

            if (!inputMap.isEmpty()) {
                csvInputList.add(inputMap);
            }
        }
        return csvInputList;
    }
}
