
#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This program is a very simple example of how to use Work Queue.
# It accepts a list of files on the command line.
# Each file is compressed with gzip and returned to the user.

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

  # Usually, we can execute the gzip utility by simply typing its name at a
  # terminal. However, this is not enough for work queue; we have to
  # specify precisely which files need to be transmitted to the workers. We
  # record the location of gzip in 'gzip_path', which is usually found in
  # /bin/gzip or /usr/bin/gzip.

  script_path = "home/ec2-user/webcrawl/CRAWLINGINMYWEB/crawl.py"
  if not os.path.exists(script_path):
    script_path = "./crawl.py"
    if not os.path.exists(script_path):
        print "crawl.py was not found. Please modify the script_path variable accordingly."       
        sys.exit(1);

  # We create the tasks queue using the default port. If this port is already
  # been used by another program, you can try setting port = 0 to use an
  # available port.
  try:
      q = WorkQueue(port)
  except:
      print "Instantiation of Work Queue failed!"
      sys.exit(1)

  print "listening on port %d..." % q.port
  link = sys.argv[1] 
  
  #Preparing for first time run; 
  depth = int(sys.argv[2])
  dep_str = "all_links.txt"
  dep_file = open(dep_str, "w+")
  dep_file.write("%s\n" % link)
  dep_file.close()
  count = 0 
  # We create and dispatch a task for each filename given in the argument list
  for i in range(0, depth):
      #dep_file = "%s_depth%d.txt" % (sys.argv[i], i)

      # Note that we write ./gzip here, to guarantee that the gzip version we
      # are using is the one being sent to the workers.

    with open(dep_str, "r") as dep_file:
      for line in dep_file:
        link = line.strip('\n')
        outfile = "output_%d.txt" % count
        count += 1
        command = "python ./crawl.py %s %s" % (link, outfile)

        t = Task(command)

      # gzip is the same across all tasks, so we can cache it in the workers.
      # Note that when specifying a file, we have to name its local name
      # (e.g. gzip_path), and its remote name (e.g. "gzip"). Unlike the
      # following line, more often than not these are the same.
        t.specify_file(script_path, "crawl.py", WORK_QUEUE_INPUT, cache=True)

      # files to be compressed are different across all tasks, so we do not
      # cache them. This is, of course, application specific. Sometimes you may
      # want to cache an output file if is the input of a later task.
    
        t.specify_file(outfile, outfile, WORK_QUEUE_OUTPUT, cache=False)

      # Once all files has been specified, we are ready to submit the task to the queue.
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
    
    #Opens the file where all the links of the current depth are to be kept
    
    with open(dep_str, "w") as dep_file:
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
    #dep_str = "%d_depth%d.txt" % (depth, i + 1)
    count = 0
  print "all tasks complete!"

  #work queue object will be garbage collected by Python automatically when it goes out of scope
  sys.exit(1)
