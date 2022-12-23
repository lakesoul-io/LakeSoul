from jproperties import Properties
import sys
import os

configs = Properties()

with open(os.path.join(sys.argv[1], 'target/maven-archiver/pom.properties'), 'rb') as config_file:
    configs.load(config_file)
    print(f'{configs.get("artifactId").data}-{configs.get("version").data}.jar')
