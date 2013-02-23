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
