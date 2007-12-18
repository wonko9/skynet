Skynet
	http://skynet.rubyforge.org/
	by Adam Pisoni

== DESCRIPTION:

Skynet is an open source Ruby implementation of Google's Map/Reduce framework, created at Geni. With Skynet, one can easily convert a time-consuming serial task, such as a computationally expensive Rails migration, into a distributed program running on many computers.

Skynet is an adaptive, self-upgrading, fault-tolerant, and fully distributed system with no single point of failure. It uses a "peer recovery" system where workers watch out for each other. If a worker dies or fails for any reason, another worker will notice and pick up that task. Skynet also has no special 'master' servers, only workers which can act as a master for any task at any time. Even these master tasks can fail and will be picked up by other workers.

== INSTALLATION:

Skynet can be installed via RubyGems:

  $ sudo gem install skynet

== GETTING STARTED
Skynet works by putting "tasks" on a message queue which are picked up by skynet workers, who execute the tasks, then put their results back on the message queue.   Skynet works best when it runs with your code.  For example, you might have a rails app and want some code you've already written to run asynchronously or in a distributed way.   Skynet can run within your code by installing a skynet launcher into your app.   Running this skynet launcher within your app guarantees all skynet workers will have access to your code.   This will be covered later.

Skynet currently supports 2 message queue systems, TupleSpace and Mysql.   By default, the TupleSpace queue is used as it is the easiest to set up, though it is less powerful and less scaleable for large installations.  

== RUNING SKYNET FOR THE FIRST TIME
Since Skynet is a distributed system, it requires you have a skynet message queue as well as any number of skynet workers running.  To start a skynet message queue and a small number of workers:

  $ skynet

This starts a skynet tuple space message queue and 4 workers.   You can now run the Skynet Console to play with skynet a little.

  $ skynet_console

Here are some commands you can run.  
  > stats
  > manager.worker_pids
  > [1,2,3,1,1,4].mapreduce(Skynet::MapreduceTest)
  
That last command actually took whatever array you gave it and counted the number of times each element appeared in the array.  It's not a very useful task, but it shows how easy it is to use.

== RUNING SKYNET IN YOUR APPLICATION

To be really useful, you'll want to run skynet in your own application.   To do that run:

  $ skynet_install [--rails] YOUR_APP_DIRECTORY

If you pass --rails it will assume it is installing in a rails app.   Once it is installed in your application, you can run skynet with

  $ ./script/skynet
  $ ./script/skynet_console

== USAGE:

Skynet was designed to make doing easy things easy and hard things possible.   The easiest way to use skynet is to create a new class with a self.map class method.  You can optionally include self.reduce, self.reduce_partitioner, self.map_partitioner as well.  Each of those methods should expect a single array (regardless of what data you pass).   Then, simple create an array and call mapreduce on it passing your class name.   Skynet will figure out which methods your class supports and use them accordingly.

== USING SKYNET IN RAILS

Skynet includes an addition to ActiveRecord that is very powerful.   

  $ YourModel.distributed_find(:all).each(YourClass)
or
  $ YourModel.distributed_find(:all).each(:somemethod)
  
In the first example, a find is 'virtually' run with your model class, and the results are distributed to the skynet workers.  If you've implemented a self.map method in YourClass, the retrieved objects will be passed (as arrays) on all the workers.

In the second example, once the objects of YourModel are distributed, each worker merely calls :somemethod against each object.

== EXAMPLES:

== LIMITATIONS:
  

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
