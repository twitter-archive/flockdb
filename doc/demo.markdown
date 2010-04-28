
# Flock demo

This demo will walk through setting up a local development flockdb instance and interacting with it
via the ruby client. To play along, you need:

- java 1.6
- ant 1.7
- ruby 1.8
- mysql 5.0

Newer versions should work for all of the above.


## Building it

If you haven't built flockdb yet, do that first:

    $ ant


## Setting up shards

To create a set of shards for development mode, a script called `setup-env.sh` is included. Make
sure mysql is running, and set these env vars so the script can talk to mysql:

    $ export DB_USERNAME="root"
    $ export DB_PASSWORD="password"

These are also used by `config/development.conf` in flockdb.

Now run `setup-env.sh`:

    $ ./src/scripts/setup-env.sh

It kills and restarts flockdb, creates the `flockdb_development` database if necessary, and runs
`flocker.rb` to create shard configurations for graphs 1-15.

You can tell flockdb is running because it will create a `flock.log` file in the current folder, and
it will respond to `server_info` queries:

    $ curl localhost:9990/server_info.txt
    build: 20100427-172426
    build_revision: 3156f9d26776bd2ddb02e78385e92cf1a271abb2
    name: flockdb
    start_time: Tue Apr 27 17:24:27 PDT 2010
    uptime: 155343
    version: 1.0

You should also be able to see that `flocker.rb` created a forward and backward shard for each of 15
made-up graphs, by asking it to show you the forwarding table:

    $ ./src/scripts/flocker.rb -D show
    GRAPH  BASE_USER_ID    SHARD
      1 000000000000000 -> localhost/forward_1
      1 000000000000000 <- localhost/backward_1
      2 000000000000000 -> localhost/forward_2
    ...
     15 000000000000000 -> localhost/forward_15
     15 000000000000000 <- localhost/backward_15

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

You can ask flocker where a shard is stored:

    $ ./src/scripts/flocker.rb -D find 1 --graph 1
    User_id 1, graph 1
      Forward: localhost/forward_1
      Backward: localhost/backward_1

In development mode, all forward edges from graph 1 are stored in a single table, so we didn't
really need to ask, but it can be useful when you have a lot of shards for a graph.

The `-D` means "development mode", which tells it that the flockdb is probably running on localhost.

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


## Migrations

As a last demo, let's create a few shards for a new graph "99", add some data, and then migrate it
to a new database.

To create 10 shards for the new graph:

    $ ./src/scripts/flocker.rb -D mkshards 10 "localhost" --graph 99
    Creating shards...
    ..........Done.

And to verify that they were created:

    $ ./src/scripts/flocker.rb -D show --graph 99
    GRAPH  BASE_USER_ID    SHARD
     99 000000000000000 -> edges-replica(localhost/edges_forward_99_000_A)
     99 199999999999999 -> edges-replica(localhost/edges_forward_99_001_A)
     99 333333333333332 -> edges-replica(localhost/edges_forward_99_002_A)
     99 4cccccccccccccb -> edges-replica(localhost/edges_forward_99_003_A)
     99 666666666666664 -> edges-replica(localhost/edges_forward_99_004_A)
     99 7fffffffffffffd -> edges-replica(localhost/edges_forward_99_005_A)
     99 999999999999996 -> edges-replica(localhost/edges_forward_99_006_A)
     99 b3333333333332f -> edges-replica(localhost/edges_forward_99_007_A)
     99 cccccccccccccc8 -> edges-replica(localhost/edges_forward_99_008_A)
     99 e66666666666661 -> edges-replica(localhost/edges_forward_99_009_A)
     99 000000000000000 <- edges-replica(localhost/edges_backward_99_000_A)
     99 199999999999999 <- edges-replica(localhost/edges_backward_99_001_A)
     99 333333333333332 <- edges-replica(localhost/edges_backward_99_002_A)
     99 4cccccccccccccb <- edges-replica(localhost/edges_backward_99_003_A)
     99 666666666666664 <- edges-replica(localhost/edges_backward_99_004_A)
     99 7fffffffffffffd <- edges-replica(localhost/edges_backward_99_005_A)
     99 999999999999996 <- edges-replica(localhost/edges_backward_99_006_A)
     99 b3333333333332f <- edges-replica(localhost/edges_backward_99_007_A)
     99 cccccccccccccc8 <- edges-replica(localhost/edges_backward_99_008_A)
     99 e66666666666661 <- edges-replica(localhost/edges_backward_99_009_A)

(Flocker assumes that most shards will be replicated, so when creating shards manually, it always
puts them behind a replicating shard.)

Make sure the local flockdb instance reloads the forwarding tables:

    $ ./src/scripts/flocker.rb -D push
    Reloading forwardings on localhost

Make a client with our new graph, and add some edges:

    >> flock = Flock.new "localhost:7915", :graphs => { :loves => 99 }
    >> flock.add(300, :loves, 400)
    >> flock.add(600, :loves, 800)
    >> flock.add(123456, :loves, 800)

What shard is user 123456 on?

    $ ./src/scripts/flocker.rb -D --graph 99 find 123456 --ids
    User_id 123456, graph 99
      Forward: [132148697]edges-replica([508882612]localhost/edges_forward_99_008_A)
      Backward: [978782308]edges-replica([844087219]localhost/edges_backward_99_008_A)

Hm, but localhost has been behaving strangely lately. Let's move that shard to 127.0.0.1, which is
really lightly loaded.

    $ ./src/scripts/flocker.rb -D migrate-one 508882612 127.0.0.1
    Setup migration: shard 508882612 -> 996146524
    Ready to push new shard config to flapps (localhost)? [Y] 
    Reloading forwardings on localhost
    Ready to start the migration? [Y] 
    Migration started!
    No busy shards.

The migration happens really quick because there's hardly any data in the shard.

    $ ./src/scripts/flocker.rb -D show --graph 99
    GRAPH  BASE_USER_ID    SHARD
     99 000000000000000 -> edges-replica(localhost/edges_forward_99_000_A)
     99 199999999999999 -> edges-replica(localhost/edges_forward_99_001_A)
     99 333333333333332 -> edges-replica(localhost/edges_forward_99_002_A)
     99 4cccccccccccccb -> edges-replica(localhost/edges_forward_99_003_A)
     99 666666666666664 -> edges-replica(localhost/edges_forward_99_004_A)
     99 7fffffffffffffd -> edges-replica(localhost/edges_forward_99_005_A)
     99 999999999999996 -> edges-replica(localhost/edges_forward_99_006_A)
     99 b3333333333332f -> edges-replica(localhost/edges_forward_99_007_A)
     99 cccccccccccccc8 -> edges-replica(127.0.0.1/edges_forward_99_008_A_copy)
     99 e66666666666661 -> edges-replica(localhost/edges_forward_99_009_A)
     99 000000000000000 <- edges-replica(localhost/edges_backward_99_000_A)
     99 199999999999999 <- edges-replica(localhost/edges_backward_99_001_A)
     99 333333333333332 <- edges-replica(localhost/edges_backward_99_002_A)
     99 4cccccccccccccb <- edges-replica(localhost/edges_backward_99_003_A)
     99 666666666666664 <- edges-replica(localhost/edges_backward_99_004_A)
     99 7fffffffffffffd <- edges-replica(localhost/edges_backward_99_005_A)
     99 999999999999996 <- edges-replica(localhost/edges_backward_99_006_A)
     99 b3333333333332f <- edges-replica(localhost/edges_backward_99_007_A)
     99 cccccccccccccc8 <- edges-replica(localhost/edges_backward_99_008_A)
     99 e66666666666661 <- edges-replica(localhost/edges_backward_99_009_A)

    robey@arya:~/twitter/flockdb (master)$ ./src/scripts/flocker.rb -D --graph 99 find 123456 --ids
    User_id 123456, graph 99
      Forward: [132148697]edges-replica([996146524]127.0.0.1/edges_forward_99_008_A_copy)
      Backward: [978782308]edges-replica([844087219]localhost/edges_backward_99_008_A)

    >> flock.select(123456, :loves, nil).to_a
    => [800]


## The end
