
# FlockDB

FlockDB is a distributed graph database for storing adjancency lists, with goals of supporting:

- a high rate of add/update/remove operations
- potientially complex set arithmetic queries
- paging through query result sets containing millions of entries
- ability to "archive" and later restore archived edges
- horizontal scaling including replication
- online data migration

Non-goals include:

- multi-hop queries (or graph-walking queries)
- automatic shard migrations

FlockDB is much simpler than other graph databases such as neo4j because it tries to solve fewer
problems. It scales horizontally and is designed for on-line, low-latency, high throughput
environments such as web-sites.

Twitter uses FlockDB to store social graphs (who follows whom, who blocks whom) and secondary
indices. As of April 2010, the Twitter FlockDB cluster stores 13+ billion edges and sustains peak
traffic of 20k writes/second and 100k reads/second.


# It does what?

If, for example, you're storing a social graph (user A follows user B), and it's not necessarily
symmetrical (A can follow B without B following A), then FlockDB can store that relationship as an
edge: node A points to node B. It stores this edge with a sort position, and in both directions, so
that it can answer the question "Who follows A?" as well as "Whom is A following?"

This is called a directed graph. (Technically, FlockDB stores the adjacency lists of a directed
graph.) Each edge has a 64-bit source ID, a 64-bit destination ID, a state (normal, removed,
archived), and a 32-bit position used for sorting. The edges are stored in both a forward and
backward direction, meaning that an edge can be queried based on either the source or destination
ID.

For example, if node 134 points to node 90, and its sort position is 5, then there are two rows
written into the backing store:

    forward: 134 -> 90 at position 5
    backward: 90 <- 134 at position 5

If you're storing a social graph, the graph might be called "following", and you might use the
current time as the position, so that a listing of followers is in recency order. In that case, if
user 134 is Nick, and user 90 is Robey, then FlockDB can store:

    forward: Nick follows Robey at 9:54 today
    backward: Robey is followed by Nick at 9:54 today

The (source, destination) must be unique: only one edge can point from node A to node B, but the
position and state may be modified at any time. Position is used only for sorting the results of
queries, and state is used to mark edges that have been removed or archived (placed into cold
sleep).


# Building

In theory, building is as simple as

    $ ant

but there are some pre-requisites. You need:

- java 1.6
- ant 1.7
- thrift 0.2.0

In addition, the tests require a local mysql instance to be running, and for `DB_USERNAME` and
`DB_PASSWORD` env vars to contain login info for it. You can skip the tests if you want:

    $ ant -Dskip.test=1

There should be support for building with sbt "soon".


# Running

Check out [the demo](http://github.com/twitter/flockdb/blob/master/doc/demo.markdown) for instructions on how to start up a local development
instance of FlockDB. It also shows how to add edges, query them, etc.


# Community

- Twitter: #flockdb
- IRC: #twinfra on freenode (irc.freenode.net)
- Mailing list: <flockdb@googlegroups.com> [subscribe](http://groups.google.com/group/flockdb)


# Contributors

- Nick Kallen @nk
- Robey Pointer @robey
- John Kalucki @jkalucki
- Ed Ceaser @asdf
