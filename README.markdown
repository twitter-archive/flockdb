# WARNING

This is in the process of being packaged for "outside of twitter use". It is very rough as code is being pushed around. please forgive the mess.

# WHAT THE HELL IS THIS

This is a distributed graph database. we use it to store social graphs (who follows whom, who blocks whom) and secondary indices at twitter.

# HOW TO RUN TESTS

mysql> CREATE DATABASE edges_test;
mysql> CREATE DATABASE flock_edges_test;

% ant test -DDB_USERNAME=fixme -DDB_PASSWORD=fixmetoo

# HOW TO ACTUALLY USE

we have not yet pulled over the main file so it wont actually start a thrift service. so it can't yet be used.
but once we do that, you start up the process, then run flocker.rb (our command line tool) to create some new graph configurations and you start manipulating data.

the ruby client (gem forthcoming) works like this:

    nk = User.find_by_screen_name('nk')
    robey = User.find_by_screen_name('robey')
    john = User.find_by_screen_name('jkalucki')
    ed = User.find_by_screen_name('asdf')

    # insert some data:
    Flock.add(nk.id, :follows, robey.id)
    Flock.add(nk.id, :follows, john.id)
    Flock.add(robey.id, :follows, nk.id)
    Flock.add(ed.id, :follows, john.id)

    # query some data:
    Flock.contains(nk.id, :follows, robey.id) # => true
    Flock.select(nk.id, :follows, nil) # => [john.id, robey.id] # mnemonic: "nick follows who?"
    Flock.select(nil, :follows, john.id) # => [nk.id, ed.id] # mnemonic: "who follows john?"

    # set algebra:
    Flock.select(nil, :follows, robey.id).intersect(nil, :follows, john.id) # => [nk.id] # mnemonic who follows both robey and john?
    # you can do `intersection`, `union`, and `difference` queries.
    # this is all done "server-side" so as to avoid transmitting huge data.

    # some hints for performance:
    Flock.select(nk.id, :follows, nil).paginate(1000).each { ... } # => gather all results, 1000 items at a time

    # pagination, 20 items per page:
    nick_follows_who = Flock.select(nk.id, :follows, nil)
    first_page, next_cursor, prev_cursor = nick_follows_who.paginate(20, :start).unapply
    second_page, next_cursor, prev_cursor = nick_follows_who.paginate(20, next_cursor).unapply

    # perform a "mass-action"
    Flock.delete(nk.id, :follows, nil) # => have nick unfollow everybody

    # edges have multiple "colors". we do this for spam:
    Flock.archive(nk.id, :follows, nil) # => archive all edges emanating from nick
    Flock.archive(nk.id, :follows, nil) # => unarchive all edges emanating from nick. this will restore all archived edges that WEREN'T deleted.

    # perform a transaction/bulk-write:
    Flock.transaction do |t|
      t.add(robey.id, :blocks, nk.id)
      t.delete(nk.id, :follows, robey.id)
      t.delete(nk.id, :follows_on_his_phone, robey.id)
      t.delete(robey.id, :follows, nick.id)
      t.delete(robey.id, :follows_on_his_phone, nick.id)
    end

# CONTRIBUTORS

* Nick Kallen
* Robey Pointer
* John Kalucki
* Ed Ceaser