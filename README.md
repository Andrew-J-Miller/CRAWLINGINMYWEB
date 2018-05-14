Andrew Miller, Grant Kalfus

lab3-3.py
crawl.py

README

lab3-3.py is a master program for workqueue. It is a distributed web crawler. It takes arguements as follows:

$ python lab3-3.py link depth

where link is a working url link and depth is the depth you wish to go for links. Each worker will be passed a cached version of crawl.py to locate all links within the layer they are in. They are also passed in an outfile which all the links the worker gathers are written to. From there, all links from all workers are written to a larger text file in the master program. If the inital link a worker is given is broken, the worker will simply exit. Both python programs are written in python 2.7, and will only work in python 2.7. 


When testing this with a local worker on an ec2 instance, we found we found a single worker could compile an upwards of 8000 links in only a few minutes. When connecting two different workers to the master from separate ec2 instances, we found that the same number of links was gathered in around one minute.
