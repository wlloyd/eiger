Eiger is a research prototype that provides stronger consistency
(causal consistency) and semantics (read- and write-only transactions)
for low-latency geo-replicated storage.  It is a fork of Cassandra.


Release Information
-------------------

The code is released in its current research-quality form.  I plan on
improving it in the future if time allows.  I stripped out the
parameterization of the Facebook workload for now, because I'm not
sure if I'm allowed to release it.


Paper Abstract
--------------

We present the first scalable, geo-replicated storage sys- tem that
guarantees low latency, offers a rich data model, and provides
“stronger” semantics. Namely, all client requests are satisfied in the
local datacenter in which they arise; the system efficiently supports
useful data model abstractions such as column families and counter
columns; and clients can access data in a causally-consistent fashion
with read-only and write-only transactional support, even for keys
spread across many servers.  The primary contributions of this work
are enabling scalable causal consistency for the complex column-
family data model, as well as novel, non-blocking al- gorithms for
both read-only and write-only transactions. Our evaluation shows that
our system, Eiger, achieves low latency (single-ms), has throughput
competitive with eventually-consistent and non-transactional Cassandra
(less than 7% overhead for one of Facebook’s real-world workloads),
and scales out to large clusters almost linearly (averaging 96%
increases up to 128 server clusters).


More Information
----------------

Our paper on Eiger has more information:
http://www.cs.princeton.edu/~wlloyd/papers/eiger-nsdi13.pdf


Requirements
------------
  * Java >= 1.6 (OpenJDK has been tested)


Getting started
---------------

Please see the documentation for Cassandra.

Running Eiger
-------------
If you're just trying to a run a small cluster on your own machine then you want to use the cassandra_dc_launcher.bash script. If you're trying to run on a real cluster of machine checkout the kodiak_dc_launcher.bash script. The local script has only been tested on macs. And both still have somethings hard coded in them (or in the base configuration files) that need to be changed, see the other open issues for some pointers.

Fork Point From Cassandra & Tips For Understanding Our Changes
--------------------------------------------------------------
Forked from 44a7db706885430128d305adb56899595eccc8ae, which is just a few commits after the cassandra 1.0.7 release.

The best places to look at my new code to see where things change would be the thrift interface (this is only for client<->server communication):
interface/cassandra.thrift

and the new code:
git diff --stat 44a7db706885430128d305adb56899595eccc8ae src/java/org/apache/cassandra/* | sort -nrk 3

and the tests I wrote:
git diff --stat 44a7db706885430128d305adb56899595eccc8ae test/unit/org/apache/cassandra/ | sort -nrk 3

The other thing to know starting off is that server<->server communication is done by sending XYZMessages that are handled by XYZVerbHandlers.

