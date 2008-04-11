Skynet
	http://skynet.rubyforge.org/
	by Adam Pisoni and Geni.com

== DESCRIPTION:

Skynet is an open source Ruby implementation of Google's Map/Reduce framework, created at Geni. With Skynet, one can easily convert a time-consuming serial task, such as a computationally expensive Rails migration, into a distributed program running on many computers.

Skynet is an adaptive, self-upgrading, fault-tolerant, and fully distributed system with no single point of failure. It uses a "peer recovery" system where workers watch out for each other. If a worker dies or fails for any reason, another worker will notice and pick up that task. Skynet also has no special 'master' servers, only workers which can act as a master for any task at any time. Even these master tasks can fail and will be picked up by other workers.

== DOCUMENTATION

Feel free to read on if you want more of an overview of Skynet with some specific examples. More specific documentation can be found here:

Skynet::Job - The main interface to Skynet.  These docs include an example of how to use Skynet.

Skynet::Config - Configuration Options

bin/skynet[link:files/bin/skynet.html] - Starting Skynet

bin/skynet_install[link:files/bin/skynet_install.html] - Installing Skynet into a local project

== Map/Reduce

First of all, Skynet is merely a distributed computing system that allows you to follow the map/reduce steps.  You don't have to use it as a map/reduce framework.  You can use it as a simple distributed system, or even a simple asynchronous processing system.

If you already know what Map/Reduce is, skip this section.                                                             

If you want to know where all this Map/Reduce hype started, you should read Google's paper on it.  http://labs.google.com/papers/mapreduce.html

When I first read that Google paper some years ago, I was a little confused about what all the hypes was.   At the most basic level, it seemed too simple to be revolutionary.  So you've got a job with 3 steps,  you put some data in, it gets split out to a map step run no many machines, the returned data gets reshuffled and parceled out to a reduce step run on many machines.  All the results are then put together again.   You can see it as 5 steps actually.   Data -> Partition -> Map -> Partition -> Reduce.  Simple enough.  Almost too simple.   It was only years later when I began working on Skynet that I realize what the revolutionary part of Google's framwork was.  It made distributed computing accessible.   Any engineer could write a complex distributed system without needing to know about the complexities of such systems.   Also, since the distributed system was generalized, you would only need one class of machines to run ALL of your distributed processing, instead of specialized machines for specialized functions.  THAT was revolutionary.   

There are a number of key differences between Google's MR system and skynet.  Firstly, currently you can not actually send raw code to the workers.  You are really only telling it where the code is.   At first this bothered me a lot.  Then I realized that in most OO systems, the amount of code you'd need duplicate and to send over the wire to every worker could be ridiculous.  For example, if you want to distribute a task you need to run in Rails, you'd have to send almost all of your app and rails to every worker with every chunk of data.  So, even if you COULD send code, you'd probably only be sending code that just called some other code in your system.  If you can't send ALL the code it needs, then you might as well just tell it where the code is.

The second big difference is that Google's MR framework uses Master federater processes to dole out tasks, recombine them, and generally watch the system.  Skynet has not such masters.   Instead Skynet uses a standard message queue for all communication.  That same message queue allows workers to watch each other in the same way a master would, but without the single point of failure (except the queue itself). 

At its simplest level, a single map reduce job defines a data set, a map method and a reduce method. It may also define a partition method. The map/reduce server evenly splits up (partitions) the data given to it and sends those chunks of data, along with a copy of the code in the map method, to workers that execute the map method against the data it was given. The output from each worker is sent back to the map/reduce server. At this point the Mapreduce server evenly partitions the RESULT data returned from the workers and sends those chunks of data along with the reduce code to the workers to be executed. The reducers return the final result which is returned to whomever requested the job be done in the first place. Not all job need a reduce step, some may just have a map step.

The most common example of a mapreduce job is a distributed word counter. Say you wanted to determine how many times a single word appears in a 1GB text file. The map/reduce server would break up the 1GB file into reasonable chunks, say 100 lines per chunk (or partition) and then send each 100 line partition along with the code that looks for that word, to workers. Each worker would grab its partition of the data, count how many times the word appears in the data and return that number. It might take dozens of workers to complete the task. When the map step is done, you are left with a huge list of counts returned by the workers. In this example, the reduce step would involve sending that list of counts to another worker, with the code required to sum those counts and finally return the total. In this way a task that used to be done in a linear fashion can be parallelized easily.

== INSTALLATION:

Skynet can be installed via RubyGems:

  $ sudo gem install skynet

== GETTING STARTED

Skynet works by putting "tasks" on a message queue which are picked up by skynet workers, who execute the tasks, then put their results back on the message queue.   Skynet works best when it runs with your code.  For example, you might have a rails app and want some code you've already written to run asynchronously or in a distributed way.   Skynet can run within your code by installing a skynet launcher into your app.   Running this skynet launcher within your app guarantees all skynet workers will have access to your code.   This will be covered later.

Skynet currently supports 2 message queue systems, TupleSpace and Mysql.   By default, the TupleSpace queue is used as it is the easiest to set up, though it is less powerful and less scaleable for large installations.  

== RUNING SKYNET FOR THE FIRST TIME
Since Skynet is a distributed system, it requires you have a skynet message queue as well as any number of skynet workers running.  To start a skynet message queue and a small number of workers:

  $ skynet

This starts a skynet tuple space message queue and 4 workers.   You can now run the skynet console to play with skynet a little.  See Skynet::ConsoleHelper for commands.

  $ skynet console
  
For help try:
  $ skynet --help
or
  $ skynet console --help

Here are some commands you can run in the skynet console.
  > stats
  > manager.worker_pids
  > [1,2,3,1,1,4].mapreduce(Skynet::MapreduceTest)
  
That last command actually took whatever array you gave it and counted the number of times each element appeared in the array.  It's not a very useful task, but it shows how easy it is to use.

For more information on creating your own Skynet jobs read the Skynet::Job documentation.

== RUNING SKYNET IN YOUR APPLICATION

To be really useful, you'll want to run skynet in your own application.   To do that run:

  $ skynet_install [--rails] YOUR_APP_DIRECTORY

If you pass --rails it will assume it is installing in a rails app.   Once it is installed in your application, you can run skynet with

  $ ./script/skynet
  $ ./script/skynet console

== USAGE:

Skynet was designed to make doing easy things easy and hard things possible.   The easiest way to use skynet is to create a new class with a self.map class method.  You can optionally include self.reduce, self.reduce_partitioner, self.map_partitioner as well.  Each of those methods should expect a single array (regardless of what data you pass).   Then, simple create an array and call mapreduce on it passing your class name.   Skynet will figure out which methods your class supports and use them accordingly.

== USING SKYNET IN RAILS

Skynet includes an addition to ActiveRecord that is very powerful.   

=== distributed_find         

  $ YourModel.distributed_find(:all).each(YourClass)
or
  $ YourModel.distributed_find(:all).each(:somemethod)
  
In the first example, a find is 'virtually' run with your model class, and the results are distributed to the skynet workers.  If you've implemented a self.map method in YourClass, the retrieved objects will be passed (as arrays) on all the workers.

In the second example, once the objects of YourModel are distributed, each worker merely calls :somemethod against each object.

=== send_later

  $ model_object.send_later(:method,options,:save)
  
Sometimes you have a method you want to call on a model asynchronously.  Using :send_later you can call a method, pass it options, and decide whether you want Skynet to save that model or not once its done calling your method.

== Creating Skynet Jobs

The main interface to Skynet is through Skynet::Job

  job = Skynet::Job.new(options)
  job.run
  
There are many options you can pass or change once you have a job object.  See Skynet::Job for more info.
  
Most of the time, you will only need to pass a map_reduce_class and map_data.  All other options just give you finer grain control.   map_data must be an array.   The map_reduce_class must AT LEAST implement a self.map class method.  It may also implement self.reduce, self.reduce_partitioner, and self.map_partitioner.  Skynet will assume it can use all of those methods in the map_reduce_class you pass.

Your map and reduce class methods should ALWAYS assume they are being passed an array.  Your map method must always return an array as well.

== Skynet Logging

  You might be interested in seeing what skynet is doing.  There are 2 Skynet::Config options which control logging.  
  Skynet::CONFIG[:SKYNET_LOG_LEVEL] and Skynet::CONFIG[:SKYNET_LOG_FILE]
  Skynet::CONFIG[:SKYNET_LOG_LEVEL] is set to Logger::ERROR by default.  Other possibilities are Logger::DEBUG, Logger::INFO, Logger::WARN, Logger::ERROR, Logger::FATAL 

  You might try Logger::INFO to see more of what's going on.  To use the Skynet::Logger inside your own classes simple
    include SkynetDebugger

SkynetDebugger[link:files/lib/skynet/skynet_debugger_rb.html]    

== CREDITS

There are a number of people who either directly or indirectly worked on Skynet.
* John Beppu (wrote the original worker/manager code)
* Justin Balthrop
* Zack Parker
* Amos Elliston
* Zack Hobson
* Alan Braverman
* Mike Stangel
* Scott Steadman
* Andrew Arrow
* Jason Rojas

Skynet was inspired by and heavily influenced by Josh Carter and this blog post.
http://multipart-mixed.com/software/simple_mapreduce_in_ruby.html

Also by Starfish by Lucas Carlson
http://tech.rufy.com/2006/08/mapreduce-for-ruby-ridiculously-easy.html
http://rufy.com/starfish/doc/

== CONTACT:
  Adam Pisoni, Geni.com (apisoni at geni.com)

== LICENSE:

(The MIT License)

Copyright (c) 2007 Adam Pisoni, Geni.com

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
