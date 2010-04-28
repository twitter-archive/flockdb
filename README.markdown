
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


# Building

In theory, building is as simple as

    $ ant

but there are some pre-requisites. You need:

- java 1.6
- ant 1.7

In addition, the tests require a local mysql instance to be running, and for `DB_USERNAME` and
`DB_PASSWORD` env vars to contain login info for it. You can skip the tests if you want:

    $ ant -Dskip.test=1

There should be support for building with sbt "soon".


# Running

Check out [the demo](doc/demo.markdown) for instructions on how to start up a local development
instance of FlockDB. It also shows how to add edges, query them, etc.


# Community

- IRC: #twinfra on freenode (irc.freenode.net)
- mailing list: TBA


# Contributors

- Nick Kallen
- Robey Pointer
- John Kalucki
- Ed Ceaser





