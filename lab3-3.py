#Lab3-3.py | This is a master python script for a distrubuted web crawler using WorkQueue 
#Submission for Lab3 part 3 as part of Advanced Computer Arch
#Grant Kalfus and Andrew Miller
# May 13 2018

#This is the master file for a distrubuted web crawler that takes a link and how many pages deep to grab links from 
#as paramters.

from work_queue import *

import os
import sys

# Main program
if __name__ == '__main__':
  port = WORK_QUEUE_DEFAULT_PORT

  if len(sys.argv) < 3:
    print "For arguments, please pass in: lab3-3.py link depth"
    print "The given link will be webcrawled 'depth' deep"
    sys.exit(1)

  #The script that will be passed to the workers is called crawl.py; it takes a given link, finds all links on the page,
  #then returns them as a text file. This checks to see if the script is in a couple of known directories. 
  script_path = "home/ec2-user/webcrawl/CRAWLINGINMYWEB/crawl.py"
  if not os.path.exists(script_path):
    script_path = "./crawl.py"
    if not os.path.exists(script_path):
        print "crawl.py was not found. Please modify the script_path variable accordingly."       
        sys.exit(1);

  # We create the tasks queue using the default port. If that port is in use, it can be changed by the user.
  try:
      q = WorkQueue(port)
  except:
      print "Instantiation of Work Queue failed!"
      sys.exit(1)

      
  print "listening on port %d..." % q.port
  
  #Gets the link from the list of arguments
  link = sys.argv[1] 
  
  #Preparing for first time run in likely the worst way possible 
  depth = int(sys.argv[2])
  dep_str = "%d_depth%d.txt" % (depth, 0)
  dep_file = open(dep_str, "w+")
  dep_file.write("%s\n" % link)
  dep_file.close()
  
  #Variable to keep track of how many text files are returned
  count = 0 
  #For however many depths the user defined...
  for i in range(0, depth):
    #Open a file containing all the links to give to the workers
    with open(dep_str, "r") as dep_file:
      for line in dep_file:
        #Remove the newline from the link
        link = line.strip('\n')
        
        #Create an output file where all found links will be placed
        outfile = "output_%d.txt" % count
        
        #Incr count to keep files distinct
        count += 1
        
        #Format the command to corrispond to given link
        command = "python ./crawl.py %s %s" % (link, outfile)

        #Create the task from the given command
        t = Task(command)

        #Since the same script is running for all iterations, we allow 
        #the workers to cache the file to speed up future iterations
        t.specify_file(script_path, "crawl.py", WORK_QUEUE_INPUT, cache=True)
      
        #Since all output files will be different, we do not want them to be cached
        t.specify_file(outfile, outfile, WORK_QUEUE_OUTPUT, cache=False)

        #Once we are done preparing the file, we submit it to the queue
        taskid = q.submit(t)
        print "submitted task (id# %d): %s" % (taskid, t.command)

    #Wait for all tasks of current depth to complete
    print "waiting for tasks of depth %d to complete..." % i
    while not q.empty():
      t = q.wait(5)
      if t:
          print "task (id# %d) complete: %s (return code %d)" % (t.id, t.command, t.return_status)
          if t.return_status != 0:
            None
    #These loops take all the generated output files and merges them down to a single file containing all links
    #Opens the file where all the links of the current depth are to be kept
    dep_str = "%d_depth%d.txt" % (depth, i)
    with open(dep_str, "w+") as dep_file:
      #For all of the returned files,
      for j in range(0, count):
        #Try to open a returned file
        try:
          with open("output_%d.txt" % j) as retfile:
            #If the returned file was opened, append all the links to the depth file 
            for line in retfile:
              dep_file.write(line)
          #remove the returned file
          os.remove("output_%d.txt" % j)
        #If the file could not be opened or deleted, move on to the next file
        except:
          continue
    
    count = 0
  print "all tasks complete!"
  
  sys.exit(1)
