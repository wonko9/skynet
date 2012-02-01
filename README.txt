Skynet
	http://skynet.rubyforge.org/
	by Adam Pisoni and Geni.com

== DESCRIPTION

Skynet is an open source Ruby implementation of Google's MapReduce framework, created at Geni. With Skynet, one can easily convert a time-consuming serial task, such as a computationally expensive Rails migration, into a distributed program running on many computers.  If you'd like to learn more about MapReduce, see my intro at the bottom of this document.

Skynet is an adaptive, self-upgrading, fault-tolerant, and fully distributed system with no single point of failure. It uses a "peer recovery" system where workers watch out for each other. If a worker dies or fails for any reason, another worker will notice and pick up that task. Skynet also has no special 'master' servers, only workers which can act as a master for any task at any time. Even these master tasks can fail and will be picked up by other workers.

For more detailed documentation see the following:

Skynet::Job - The main interface to Skynet; includes an example of how to use Skynet

Skynet::Config - Configuration options

bin/skynet[link:files/bin/skynet.html] - Starting Skynet

bin/skynet_install[link:files/bin/skynet_install.html] - Installing Skynet into a local project

There are also some examples in the examples/ directory included with Skynet.

== INSTALLATION

Skynet can be installed via RubyGems:

  $ sudo gem install skynet

or grab the bleeding edge skynet in svn at
  $ svn checkout svn+ssh://developername@rubyforge.org/var/svn/skynet
  $ cd skynet; rake install_gem

== INITIAL SETUP

Skynet works by putting "tasks" on a message queue which are picked up by skynet workers. The workers execute tasks and put their results back on the message queue. Skynet workers need to load your code at startup in order to be able to execute your tasks. This loading is handled by installing a skynet config file into your app running skynet_install[link:files/bin/skynet_install.html].

  $ skynet_install [--rails] [--mysql] APP_ROOT_DIR

This creates a file called skynet_config.rb in APP_ROOT_DIR/config to which you can add the relevant requires. For example, you might have a rails app and want some of that code to run asynchronously or in a distributed way. Just run 'skynet_install --rails' in your rails root, and it will automatically create config/skynet_config.rb and require environment.rb.

Skynet currently supports 2 message queue systems, TupleSpace and Mysql. By default, the TupleSpace queue is used as it is the easiest to set up, though it is less powerful and less scaleable for large installations. If you pass --mysql to skynet_install, it will assume you are using the mysql as your message queue.

== STARTING SKYNET

Once it is installed in your application, you can run skynet from your applications root directory with:

  $ skynet start [--workers=N]

This starts a skynet tuple space message queue and 4 workers. You can control how many workers to start per machine
by passing --workers=N.

== SKYNET CONSOLE

You can now run the skynet console to play with skynet a little. See Skynet::ConsoleHelper for commands.

  $ skynet console

Remember, when you change your code, you must stop/start skynet.

For help try:
  $ skynet --help
or
  $ skynet console --help

Here are some commands you can run in the skynet console.
  > stats
  > manager.worker_pids
  > [1,2,3,1,1,4].mapreduce(Skynet::MapreduceTest)

That last command actually took whatever array you gave it and counted the number of times each element appeared in the array.  It's not a very useful task, but it shows how easy Skynet is to use.

To see what Skynet is doing, you may want to tail the skynet logs being written to your log directory.

For more information on creating your own Skynet jobs read the Skynet::Job documentation.

== USAGE

Skynet was designed to make doing easy things easy and hard things possible. The easiest way to use skynet is to create a new class with a self.map class method. You can optionally include self.reduce and self.reduce_partitioner as well. Each of those methods should expect a single array (regardless of what data you pass). Then, simply create an array and call mapreduce on it passing your class name. Skynet will figure out which methods your class supports and use them accordingly.

== USING SKYNET IN RAILS

Skynet includes an extension to ActiveRecord that is very powerful.

=== distributed_find

  $ YourModel.distributed_find(:all).each(:somemethod)

A find is 'virtually' run with your model class, and the results are distributed to the skynet workers. Each worker then calls :somemethod against each object.

=== send_later

  $ model_object.send_later(:method, options, :save)

Sometimes you have a method you want to call on a model asynchronously. Using :send_later you can call a method, pass it options, and decide whether you want Skynet to save that model or not once its done calling your method.

== Creating Skynet Jobs

The main interface to Skynet is through Skynet::Job

  job = Skynet::Job.new(options)
  job.run

There are many options you can pass or change once you have a job object. See Skynet::Job for more info.

Most of the time, you will only need to pass a :map_reduce_class and :map_data. All other options just give you finer grained control. The :map_data must be an array, and the :map_reduce_class must implement at least a self.map class method. It may optionally implement self.reduce and self.reduce_partitioner. Your map and reduce class methods should ALWAYS assume they are being passed an array. Your map method must always return an array as well.

== Skynet Logging

You might be interested in seeing what skynet is doing.  There are two Skynet::Config options which control logging: Skynet::CONFIG[:SKYNET_LOG_LEVEL] and Skynet::CONFIG[:SKYNET_LOG_FILE]. Skynet::CONFIG[:SKYNET_LOG_LEVEL] is set to Logger::ERROR by default. Other possibilities are Logger::DEBUG, Logger::INFO, Logger::WARN, Logger::ERROR, and Logger::FATAL. You might try Logger::INFO to see more of what's going on. To use the Skynet::Logger inside your own classes simply add:

  include SkynetDebugger

For more information see SkynetDebugger[link:files/lib/skynet/skynet_debugger_rb.html].

== A Note on MapReduce

At its simplest level, a MapReduce job defines a data set, a map method and a reduce method. It may also define a partition method. The MapReduce server evenly splits up (partitions) the data given to it and sends those chunks of data, along with a copy of the code in the map method, to workers that execute the map method against the data it was given. The output from each worker is sent back to the MapReduce server. At this point the MapReduce server evenly partitions the RESULT data returned from the workers and sends those chunks of data along with the reduce code to the workers to be executed. The reducers return the final result which is returned to the process that requested the job be done in the first place. Not all jobs need a reduce step, some may just have a map step.

The most common example of a MapReduce job is a distributed word counter. Say you wanted to determine how many times a single word appears in a 1GB text file. The MapReduce server would break up the 1GB file into reasonable chunks, say 100 lines per chunk (or partition) and then send each 100 line partition along with the code that looks for that word, to workers. Each worker would grab its partition of the data, count how many times the word appears in the data and return that number. It might take dozens of workers to complete the task. When the map step is done, you are left with a huge list of counts returned by the workers. In this example, the reduce step would consist of sending that list of counts to another worker, with the code required to sum those counts and finally return the total. In this way a task that used to be done in a linear fashion can be parallelized easily.

If you want more details on MapReduce, read Google's paper on it.  http://labs.google.com/papers/mapreduce.html

When I first read that Google paper some years ago, I was a little confused about what was so unique about it. At the most basic level, it seemed too simple to be revolutionary. So you've got a job with two steps: map and reduce. You put some data in, it gets split out to a map step run on many machines. the returned data gets reshuffled and parceled out to a reduce step run on many machines. All the results are then put together again. You can see it as five steps actually. Data -> Partition -> Map -> Partition -> Reduce. Simple enough. Almost too simple. It was only years later when I began working on Skynet that I realized what the revolutionary part of Google's framework was. It made distributed computing accessible. Any engineer could write a complex distributed system without needing to know about the complexities of such systems. They just write a map function and a reduce function. Also, since the distributed system was generalized, you would only need one class of machines to run ALL of your distributed processing, instead of specialized machines for specialized jobs. That WAS revolutionary.

Skynet is merely a distributed computing system that allows you to break your problem into map and reduce steps. You don't have to use it as a MapReduce framework though. You can use it as a simple distributed system, or even a simple asynchronous processing system.

There are a number of key differences between Google's MapReduce system and skynet. First, currently you can not actually send raw code to the workers. You are really only telling it where the code is. At first this bothered me a lot. Then I realized that in most Object Oriented systems, the amount of code you'd need to duplicate and send over the wire to every worker could be ridiculous. For example, if you want to distribute a task you need to run in Rails, you'd have to send almost all of your app and rails code to every worker with every chunk of data. So, even if you COULD send code, that code would probably eventually jut call some other code in your system. If you can't send ALL the code needed for a task, then you might as well just tell Skynet where all the needed code is.

The second big difference is that Google's MapReduce framework uses master federator processes to dole out tasks, recombine them, and generally watch the system. Skynet has no such masters. Instead Skynet uses a standard message queue for all communication. That message queue allows workers to watch each other in the same way a master would, but without the single point of failure (except for the queue itself).

== CREDITS

There are a number of people who either directly or indirectly worked on Skynet.
* Justin Balthrop
* Zack Parker
* Amos Elliston
* Zack Hobson
* Alan Braverman
* Mike Stangel
* Scott Steadman
* Andrew Arrow
* John Beppu (wrote the original worker/manager code)
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
