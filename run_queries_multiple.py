import subprocess
import os
import time
import pathlib
import sys
import json
from shutil import copyfile

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
        print(entry.name)
        with open(vertex_field_value_file) as f:
            lines = f.readlines()
        for line in lines:
            print(line)
            with open(entry.path) as json_file:
                data = json.load(json_file)
                json_file.close()
                graph = data["graph"]
                nodes = graph["nodes"]
                for node in nodes:
                    node_vertex_type = node["vertexType"]
                    print(node_vertex_type)
                    if vertex_type == node_vertex_type:
                        print("********")
                        filters = node["filters"]
                        for filter in filters:
                            field = filter["field"]
                            if field == vertex_field_name:
                                filter["value"] = line
                                print(line)

            with open(entry.path, "w") as jsonFile:
                print(data)
                json.dump(data, jsonFile)
                json_file.close()

            author_name = line.replace(" ", "")
            author_name = author_name.rstrip()
            print(author_name)
            json_file_author = "test_queries_sph/" + author_name + ".json"
            copyfile(entry.path, json_file_author)
           
            t0 = time.time()
            test_process = subprocess.Popen(["java", "-cp", target, className, config, json_file_author],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            test_output, test_error = test_process.communicate()
            if test_process.returncode != 0:
                print("Failure at %s" % entry.name)
                print(test_error)
            author_name = line.replace(" ", "")
            author_name = author_name.rstrip()
            print(author_name)
            outfile = "test_queries_sph/"  +author_name +  "_edges.csv"
            linecount = int(subprocess.check_output(["wc", "-l", outfile]).decode("utf8").split()[0])
            print("%s results returned in %s seconds" % (linecount, time.time() - t0))

            time.sleep(5)
