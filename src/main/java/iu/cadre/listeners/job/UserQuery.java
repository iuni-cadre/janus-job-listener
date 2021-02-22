package iu.cadre.listeners.job;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

class CSVOutput {
    String field;
    String vertexType;
}

class Edge {
    String source;
    String target;
    String relation;

    @Override
    public String toString() {
        return "Edge{ " + source + " => " + relation + " => " + target + "}";
    }
}

@VisibleForTesting
class Node {
    String type;
    ArrayList<Filter> filters;

    Node(String _type) {
        type = _type;
        filters = new ArrayList<>();
    }
}

class Filter {
    String field;
    String value;
    String operator;

    Filter() {}
    Filter(String field_, String value_)
    {
        field = field_;
        value = value_;
    }
}

public class UserQuery {
    private JsonObject _json;
    public UserQuery(JsonObject json) {
        _json = json;
    }

    public String JobId() { return _json.get("job_id").getAsString(); }
    public String UserName() { return _json.get("username").getAsString(); }
    public String JobName() { return _json.get("job_name").getAsString(); }
    public String DataSet()
    {
        JsonElement dataset = _json.get("dataset");
        return dataset == null ? "" : dataset.getAsString();
    }
    public JsonObject Graph() { return _json.get("graph").getAsJsonObject(); }
    public String UserId() { return _json.get("user_id").getAsString(); }

    public List<Node> Nodes()
    {
        JsonArray nodes = _json.get("graph").getAsJsonObject().get("nodes").getAsJsonArray();
        ArrayList<Node> result = new ArrayList<Node>();

        for (int i=0; i < nodes.size(); i++) {
            JsonObject edgeJson = nodes.get(i).getAsJsonObject();
            Node n = new Node(edgeJson.get("vertexType").getAsString());
            JsonArray filters = edgeJson.get("filters").getAsJsonArray();

                for (int j = 0; j < filters.size(); j++) {
                    Filter f = new Filter();
                    JsonObject filterField = filters.get(j).getAsJsonObject();
                    f.field = filterField.get("field").getAsString();
                    if (DataSet().equals("mag")) {
                        if (f.field.equals("title"))
                            f.field = "paperTitle"; // hopefully temporary hack
                        if (n.type.equals("Paper") && f.field.equals("name"))
                            f.field = "displayName"; // hopefully temporary hack
                        if (n.type.equals("Author") && f.field.equals("name"))
                            f.field = "displayName"; // hopefully temporary hack
                        if (n.type.equals("JournalFixed") && f.field.equals("name"))
                            f.field = "displayName"; // hopefully temporary hack
                        if (n.type.equals("ConferenceInstance") && f.field.equals("name"))
                            f.field = "displayName"; // hopefully temporary hack
                        f.value = filterField.get("value").getAsString();
                        f.operator = filterField.get("operator").getAsString();
                        n.filters.add(f);
                    }else if(DataSet().equals("wos")) { // WOS
                        f.value = filterField.get("value").getAsString();
                        f.operator = filterField.get("operator").getAsString();
                        f.field = filterField.get("field").getAsString();
                        n.filters.add(f);
                    }else {
                        f.value = filterField.get("value").getAsString();
                        f.operator = filterField.get("operator").getAsString();
                        f.field = filterField.get("field").getAsString();
                        n.filters.add(f);
                    }
                }

            result.add(n);
        }
        return result;
    }
    public List<Edge> Edges()
    {
        JsonArray edges = _json.get("graph").getAsJsonObject().get("edges").getAsJsonArray();
        ArrayList<Edge> result = new ArrayList<Edge>();
        for (int i=0; i < edges.size(); i++) {
            JsonObject edgeJson = edges.get(i).getAsJsonObject();
            Edge e = new Edge();
            e.source = edgeJson.get("source").getAsString();
            e.target = edgeJson.get("target").getAsString();
            e.relation = edgeJson.get("relation").getAsString();
            result.add(e);
        }
        return result;
    }

    public List<CSVOutput> CSV()
    {
        JsonArray fields = _json.getAsJsonObject().get("csv_output").getAsJsonArray();
        ArrayList<CSVOutput> result = new ArrayList<CSVOutput>();
        for (JsonElement field: fields) {
            JsonObject edgeJson = field.getAsJsonObject();
            CSVOutput e = new CSVOutput();
            e.field = edgeJson.get("field").getAsString();
            e.vertexType = edgeJson.get("vertexType").getAsString();
            result.add(e);
        }
        return result;
    }

    public boolean RequiresGraph()
    {
        return Edges().stream().anyMatch(e -> e.relation.matches("Citations|References"));
    }

    public boolean RequiresCitationsGraph()
    {
        return Edges().stream().anyMatch(e -> e.relation.matches("Citations"));
    }

    public boolean RequiresReferencesGraph()
    {
        return Edges().stream().anyMatch(e -> e.relation.matches("References"));
    }

    public boolean HasAbstractSearch() {
        return Nodes().stream().anyMatch(n ->
                n.filters.stream().anyMatch( f -> f.field.equals("abstract")));
    }

    @Override
    public String toString() {
        return "UserQuery{" +
               "_json=" + _json +
               '}';
    }
}
