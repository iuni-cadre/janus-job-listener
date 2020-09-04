import subprocess
import os
import time

target = "target/janus-job-listener-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
className = "iu.cadre.listeners.job.JanusConnection"
config = "../lib/cadre_config.properties"

for entry in os.scandir("queries"):
    if entry.path.endswith(".json") and entry.is_file():
        print(entry.name)
        test_process = subprocess.Popen(["java", "-cp", target, className, config, entry.path],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        test_output, test_error = test_process.communicate()
        if test_process.returncode != 0:
            print("Failure at %s" % entry.name)
            print(test_error)
        time.sleep(5)