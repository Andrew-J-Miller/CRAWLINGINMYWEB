import urllib2
import re
import os

#This is the worker python program that searches 2 links deep given a starting link and writes a text file of all links it finds to return to the master


cwd = os.getcwd()
location = cwd + "//test.txt"
try:
    website = urllib2.urlopen('http://www.starwars.wikia.com/Main_Page')
    html = website.read()
    files = re.findall('href="http.*?"',html)
    linkFile = open(location, 'w+')


    for x in files:
        p = re.compile('http.+?(?=")')
        x = p.search(x)
        x = x.group()
        linkFile.write(x)
        linkFile.write('\n')
        try:
            website2 = urllib2.urlopen(x)
            html = website2.read()
            files2 = re.findall('href="http.*?"',html)
        except:
            print x, " broken link"

        for y in files2:
            p = re.compile('http.+?(?=")')
            y = p.search(y)
            y = y.group()
            linkFile.write(y)
            linkFile.write('\n')
except:
    print "Starting link is broken. Exiting worker."
    exit()

linkFile.close()
