package iu.cadre.listeners.job;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import javax.json.Json;
import java.util.ArrayList;
import java.util.List;

class Edge {
    String source;
    String target;
    String relation;
}

class Node {
    String type;
    ArrayList<Filter> filters;
}

class Filter {
    String field;
    String value;
    String operator;
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
            Node n = new Node();
            n.type = edgeJson.get("vertexType").getAsString();
            JsonArray filters = edgeJson.get("filters").getAsJsonArray();
            n.filters = new ArrayList<>();
            for (int j = 0; j < filters.size(); j++) {
                Filter f = new Filter();
                JsonObject filterField = filters.get(j).getAsJsonObject();
                f.field = filterField.get("field").getAsString();
                if (f.field.equals("title"))
                    f.field = "paperTitle"; // hopefully temporary hack
                f.value =  filterField.get("value").getAsString();
                f.operator = filterField.get("operator").getAsString();
                n.filters.add(f);
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

    @Override
    public String toString() {
        return "UserQuery{" +
               "_json=" + _json +
               '}';
    }
}
