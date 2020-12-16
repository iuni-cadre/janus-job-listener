import subprocess
import os
import time
import pathlib
import sys

target = "target/janus-job-listener-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
className = "iu.cadre.listeners.job.JanusConnection"
config = "../lib/cadre_config.properties"

if len(sys.argv) > 1:
    config = sys.argv[1]

for entry in os.scandir("queries"):
    if entry.path.endswith(".json") and entry.is_file():
        print(entry.name)
        t0 = time.time()
        test_process = subprocess.Popen(["java", "-cp", target, className, config, entry.path],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        test_output, test_error = test_process.communicate()
        if test_process.returncode != 0:
            print("Failure at %s" % entry.name)
            print(test_error)
        outfile = "queries/" + str(pathlib.PurePath(entry.name).with_suffix('')) + "_edges.csv"
        linecount = int(subprocess.check_output(["wc", "-l", outfile]).decode("utf8").split()[0])
        print("%s results returned in %s seconds" % (linecount, time.time() - t0))

        time.sleep(5)
