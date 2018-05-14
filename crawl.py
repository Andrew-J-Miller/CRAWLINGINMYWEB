import urllib2
import re
import os
import sys

#crawl.py
#This is the worker python script that takes a link and writes a text file of all links in that layer



try:
	#Open website
    website = urllib2.urlopen(sys.argv[1])
    html = website.read()
	#A regular expression to find all https or http href links within the layer
    files = re.findall('href="http.*?"',html)
	#Open the outfile specified by the master
    linkFile = open(sys.argv[2], 'w+')


    for x in files:
		#A regular expression to format the links to just the link portion
        p = re.compile('http.+?(?=")')
        x = p.search(x)
        x = x.group()
		#Write links to outfile
        linkFile.write(x)
        linkFile.write('\n')
        

#condition for failure to open link passed to the worker			
except:
    print "Starting link is broken. Exiting worker."
    exit()


linkFile.close()
