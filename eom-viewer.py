# orders the files inside the image-cache directory and writes out symlinks.
# this makes eom (eye of mate) traverse them in order

import os
import subprocess

def sorter(v):
    if v.startswith('image-cache/articles'):
        return v
    msid = v.split('/')[1].split(':')[1]
    y = "/".join(v.split("/")[2:3])
    z = "image-cache/articles/%s/%s" % (msid,y)
    return z

results = subprocess.check_output(["find", "image-cache/", "-type", "f"])
results = [v.decode('utf-8') for v in results.split()]
results = sorted(results, key=sorter)

os.system("mkdir -p links && cd links && rm *")

for i, row in enumerate(results):
    print(row)
    os.system("cd links && ln -s %s %s" % (os.path.abspath(row), i))
    
