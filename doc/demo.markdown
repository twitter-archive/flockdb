
# Flock demo

This demo will walk through setting up a local development flockdb instance and interacting with it
via the ruby client. To play along, you need:

- java 1.6
- sbt 0.7.4
- ruby 1.8
- mysql 5.0
- [gizzmo](http://github.com/twitter/gizzmo)

Newer versions should work for all of the above. Make sure to put the gizzmo binary on your path.


## Building it

If you haven't built flockdb yet, do that first:

    $ sbt update package-dist

You may need to set `DB_USERNAME` and `DB_PASSWORD` for tests to complete (see below).


## Setting up shards

To create a set of shards for development mode, a script called `setup-env.sh` is included. Make
sure mysql is running, and set these env vars so the script can talk to mysql:

    $ export DB_USERNAME="root"
    $ export DB_PASSWORD="password"

These are also used by `config/development.conf` in flockdb.

Now run `setup-env.sh`:

    $ ./dist/flockdb/scripts/setup-env.sh

It kills and restarts flockdb, creates the `flockdb_development` database if necessary, and runs
`gizzmo` to create shard configurations for graphs 1-15.

You can tell flockdb is running because it will create a `flock.log` file in the current folder, and
it will respond to `server_info` queries:

    $ curl localhost:9990/server_info.txt
    build: 20100713-165811
    build_revision: 4b2443968d131b7967885b0b0cb62dde04ab5455
    name: flockdb
    start_time: Tue Jul 13 17:01:33 PDT 2010
    uptime: 440837
    version: 1.0.4

You should also be able to see that `gizzmo` created a forward and backward shard for each of 15
made-up graphs, by asking it to show you the forwarding table. First, set up a default host & port
in your `.gizzmorc` to make the rest of the demo easier:

    $ cat ~/.gizzmorc
    hosts: localhost
    port: 7920

Then:

    $ gizzmo forwardings
    1	0	localhost/forward_1
    -1 0 localhost/backward_1
    2	0	localhost/forward_2
    ...
    15 0 localhost/forward_15
    -15	0	localhost/backward_15

The shard config is necessary so that flockdb knows where to write edges for a graph. If no
forwarding info is provided for a graph, any operation on that graph will throw an exception.


## Talking to flockdb

Now install the ruby flockdb client:

    $ sudo gem install flockdb

The flockdb interface is thrift, so you can talk to it in many different languages, but the raw
thrift interface isn't as expressive as the one in the ruby client, which adds some nice syntactic
sugar for creating queries.

If flockdb is running, you should be able to connect with it from an `irb` ruby prompt:

    >> require "flockdb"
    => true
    >> flock = Flock.new "localhost:7915", :graphs => { :follows => 1, :blocks => 2 }
    => #<Flock::Client:0x101505aa8 @service=..., @graphs={:follows=>1, :blocks=>2}>

Okay, in an empty database, how many people are following user 1?

    >> flock.select(nil, :follows, 1).to_a
    => []

Let's make user 1 a bit more popular, then.

    >> flock.add(20, :follows, 1)
    => nil
    >> flock.add(21, :follows, 1)
    => nil
    >> flock.add(22, :follows, 1)
    => nil

Did that help?

    >> flock.select(nil, :follows, 1).to_a
    => [22, 21, 20]

Notice that the results are given in recency order, most recent first.


## Under the hood

You can ask `gizzmo` where a shard is stored:

    $ gizzmo lookup 1 1
    localhost/forward_1
    $ gizzmo lookup -- -1 1
    localhost/backward_1

In development mode, all forward edges from graph 1 are stored in a single table, so we didn't
really need to ask, but it can be useful when you have a lot of shards for a graph.

    mysql> use edges_development;
    mysql> select * from backward_1_metadata where source_id=1;
    +-----------+-------+-------+------------+
    | source_id | count | state | updated_at |
    +-----------+-------+-------+------------+
    |         1 |     3 |     0 |          0 | 
    +-----------+-------+-------+------------+
    1 row in set (0.00 sec)

So, in the backward direction, user 1 is being followed by 3 people.

    mysql> select * from backward_1_edges where source_id=1;
    +-----------+---------------------+------------+----------------+-------+-------+
    | source_id | position            | updated_at | destination_id | count | state |
    +-----------+---------------------+------------+----------------+-------+-------+
    |         1 | 1334224838599527719 | 1272415960 |             20 |     1 |     0 | 
    |         1 | 1334224842163537338 | 1272415964 |             21 |     1 |     0 | 
    |         1 | 1334224846422671757 | 1272415968 |             22 |     1 |     0 | 
    +-----------+---------------------+------------+----------------+-------+-------+
    3 rows in set (0.01 sec)

And there they are. You can look up user 20 in the forward direction (`forward_1_edges`) to see the
same edge in the forward table.


## Bundling up modifications

You can bundle up modify operations in a "transaction":

    >> flock.transaction do |t|
    ?>   t.add(1, :follows, 20)
    >>   t.add(1, :follows, 30)
    >> end
    => nil

It's not a transaction in the database sense, but just a way to bundle multiple modifications into a
single RPC call. Flockdb accepts the collection of modifications with a single "okay" and promises
to take care of all of them eventually.

FlockDB can also perform a "mass-action" on all edges going to (or from) a vertex:

    >> flock.remove(229, :follows, nil)

which can be useful when removing a vertex from the system.

Once an edge has been added to the system, it's never deleted. Instead, the state of an edge can be
changed to "removed" or "archived". Removing an edge is similar to deleting it, except that the row
isn't deleted from mysql for performance reasons.

Archiving an edge changes its state to "archived", which hides it from normal queries, but allows it
to be restored to a normal state by "un-archiving" it.

    >> flock.archive(229, :follows, nil)
    >> flock.unarchive(229, :follows, nil)


## Compound queries

To find out who's reciprocally following user 1, we can ask for the intersection of "who is
following user 1" and "who is user 1 following":

    >> flock.select(1, :follows, nil).intersect(nil, :follows, 1).to_a
    => [20]

Oh, just user 20. Well, how about the union then?

    >> flock.select(1, :follows, nil).union(nil, :follows, 1).to_a
    => [30, 22, 21, 20]

Cool. So wait, who's following user 1 that user 1 is *not* following back?

    >> flock.select(nil, :follows, 1).difference(1, :follows, nil).to_a
    => [22, 21]

Ahh okay.


## Paging through results

If the result set is really long, you may want to page through them.

    >> pager = flock.select(1, :follows, nil).union(nil, :follows, 1).paginate(2)
    => #<Flock::Operation:0x10157a538 ...>
    >> pager.next_page
    => [30, 22]
    >> pager.next_page
    => [21, 20]

For stateless interaction (like websites), you can manually retrieve the next and previous cursor:

    >> query = flock.select(1, :follows, nil).union(nil, :follows, 1)
    >> page, next_cursor, prev_cursor = query.paginate(2).unapply
    => [[30, 22], 1334246632933954956, 0]
    >> page, next_cursor, prev_cursor = query.paginate(2, next_cursor).unapply
    => [[21, 20], 0, -1334246630353519131]

The client library can also handle pagination automatically for you:

    >> flock.select(1, :follows, nil).union(nil, :follows, 1).paginate(2).each { |n| puts n }
    30
    22
    21
    20

(The client is fetching a page of 2 results at a time, and querying for the next page every time it
runs out.)


## Migrations

As a last demo, let's create a few shards for a new graph "99", add some data, and then migrate it
to a new database.

To create 10 shards for the new graph:

    $ ./src/scripts/mkshards.rb -n 10 99
    Creating bins..........Done.

And to verify that they were created:

    $ gizzmo forwardings -t 99
    99	0	localhost/edges_99_0000_forward_replicating
    99	115292150460684697	localhost/edges_99_0001_forward_replicating
    99	230584300921369394	localhost/edges_99_0002_forward_replicating
    99	345876451382054091	localhost/edges_99_0003_forward_replicating
    99	461168601842738788	localhost/edges_99_0004_forward_replicating
    99	576460752303423485	localhost/edges_99_0005_forward_replicating
    99	691752902764108182	localhost/edges_99_0006_forward_replicating
    99	807045053224792879	localhost/edges_99_0007_forward_replicating
    99	922337203685477576	localhost/edges_99_0008_forward_replicating
    99	1037629354146162273	localhost/edges_99_0009_forward_replicating

(`mkshards.rb` assumes that most shards will be replicated, so when creating shards manually, it
always puts them behind a replicating shard.)

Make sure the local flockdb instance reloads the forwarding tables:

    $ gizzmo reload
    Are you sure? Reloading will affect production services immediately! (Type 'yes')
    yes

Make a client with our new graph, and add some edges:

    >> flock = Flock.new "localhost:7915", :graphs => { :loves => 99 }
    >> flock.add(300, :loves, 400)
    >> flock.add(600, :loves, 800)
    >> flock.add(123456, :loves, 800)

What shard is user 123456 on?

    $ gizzmo --subtree lookup 99 123456
    localhost/edges_99_0008_forward_replicating
      localhost/edges_99_0008_forward_1

Hm, but localhost has been behaving strangely lately. Let's move that shard to 127.0.0.1, which is
really lightly loaded. First, create the new shard:

    $ gizzmo create -s "INT UNSIGNED" -d "INT UNSIGNED" com.twitter.flockdb.SqlShard 127.0.0.1/edges_99_0008_new
    127.0.0.1/edges_99_0008_new

Then, setup a migration:

    $ gizzmo setup-migrate localhost/edges_99_0008_forward_1 127.0.0.1/edges_99_0008_new
    localhost/edges_99_0008_new_migrate_replica

If you look at the replication subtree for `localhost/edges_99_0008_forward_replicating`, you can
see that it's added a layer of replication between the old shard and the new one, and the new one is
behind a write-only barrier:

    $ gizzmo subtree localhost/edges_99_0008_forward_replicating
    localhost/edges_99_0008_forward_replicating
      localhost/edges_99_0008_new_migrate_replica
        localhost/edges_99_0008_forward_1
        localhost/edges_99_0008_new_migrate_write_only
          127.0.0.1/edges_99_0008_new

So let's reload the forwarding table to make sure everyone starts using this replication:

    $ gizzmo reload

Now, all writes are going to both places and we can start the copy:

    $ gizzmo copy localhost/edges_99_0008_forward_1 127.0.0.1/edges_99_0008_new

The destination shard will be marked "busy" during the copy, but because we only had 3 edges in it,
the copy will probably be done before we can even check:

    $ gizzmo busy

Yep, no busy shards. We can finish the migration, then, to remove the replication layer and the
source shard.

    $ gizzmo finish-migrate localhost/edges_99_0008_forward_1 127.0.0.1/edges_99_0008_new
    $ gizzmo subtree localhost/edges_99_0008_forward_replicating
    localhost/edges_99_0008_forward_replicating
      127.0.0.1/edges_99_0008_new

Sweet! Reload to tell flockdb to stop writing to the old shard.

    $ gizzmo reload

And make sure the data is still there.

    >> flock.select(123456, :loves, nil).to_a
    => [800]


## The end
