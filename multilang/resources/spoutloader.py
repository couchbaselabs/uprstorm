import sys
import yaml
import subprocess 

config = yaml.load(file('config.yaml', 'rb'))
spout = config['spout']

index = 1
total = 1
if len(sys.argv) == 3:
    index = sys.argv[1]
    total = sys.argv[2]

subprocess.call("python {0}.py {1} {2}".format(spout, index, total) , shell=True)

