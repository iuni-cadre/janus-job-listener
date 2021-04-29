import subprocess
import os
import time
import pathlib
import sys
import json

target = "target/janus-job-listener-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
className = "iu.cadre.listeners.job.JanusConnection"
config = "./test_queries_sph/cadre_config.properties"
vertex_type = "Author"
vertex_field_name = "name"
vertex_field_value_file = "./test_queries_sph/author_names"

if len(sys.argv) > 1:
    config = sys.argv[1]
    vertex_field_value_file = sys.argv[2]

for entry in os.scandir("test_queries_sph"):
    if entry.path.endswith(".json") and entry.is_file():
        print(entry.name)_
        with open('filename') as f:
            lines = f.readlines()
        for line in lines:
            with open(entry.path) as json_file:
                data = json.load(json_file)
                graph = data["graph"]
                nodes = graph["nodes"]
                for node in nodes:
                    node_vertex_type = node["vertexType"]
                    if vertex_type is node_vertex_type:
                        filters = node["filters"]
                        for filter in filters:
                            filed = filter["field"]
                            if field is vertex_field_name:
                                filed["value"] = line

            with open(entry.path, "w") as jsonFile:
                json.dump(data, jsonFile)

            t0 = time.time()
            test_process = subprocess.Popen(["java", "-cp", target, className, config, entry.path],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            test_output, test_error = test_process.communicate()
            if test_process.returncode != 0:
                print("Failure at %s" % entry.name)
                print(test_error)
            outfile = "test_queries/" + str(pathlib.PurePath(entry.name).with_suffix('')) + "_edges.csv"
            linecount = int(subprocess.check_output(["wc", "-l", outfile]).decode("utf8").split()[0])
            print("%s results returned in %s seconds" % (linecount, time.time() - t0))

            time.sleep(5)
