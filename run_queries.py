import subprocess
import os
import time
import pathlib
import sys

target = "target/janus-job-listener-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
className = "iu.cadre.listeners.job.JanusConnection"
config = "./test_queries/cadre_config.properties"
testDir = "test_queries"

if len(sys.argv) > 1:
    config = sys.argv[1]

for entry in os.scandir(testDir):
    if entry.path.endswith(".json") and entry.is_file():
        print(entry.name)
        t0 = time.time()
        test_process = subprocess.Popen(["java", "-cp", target, className, config, entry.path],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        test_output, test_error = test_process.communicate()
        runTime = time.time() - t0
        print("Run time: %s seconds" % (runTime))

        if test_process.returncode != 0:
            print("Failure at %s" % entry.name)
            print(test_error)

        vertexFile = testDir + "/" + str(pathlib.PurePath(entry.name).with_suffix('')) + ".csv"

        if os.path.isfile(vertexFile):
           linecount = int(subprocess.check_output(["wc", "-l", vertexFile]).decode("utf8").split()[0])
           print("%s results returned in vertex file" % (linecount))
        else:
           raise FileNotFoundError("Vertex CSV file not found")

        edgeFile = testDir + "/" + str(pathlib.PurePath(entry.name).with_suffix('')) + "_edges.csv"

        if os.path.isfile(edgeFile):
           linecount = int(subprocess.check_output(["wc", "-l", edgeFile]).decode("utf8").split()[0])
           print("%s results returned in edge file" % (linecount))

        time.sleep(5)
