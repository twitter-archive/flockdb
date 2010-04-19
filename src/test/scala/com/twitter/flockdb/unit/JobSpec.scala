package com.twitter.flockdb.unit

import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.multi.{Archive, RemoveAll, Unarchive}
import jobs.single.{Add, Remove}
import shards.{BlackHoleShard, Shard, Metadata}
import thrift.Edge


class FakeLockingShard(shard: Shard) extends BlackHoleShard(null, 1, Nil) {
  override def withLock[A](sourceId: Long)(f: (Shard, Metadata) => A) = f(shard, shard.getMetadata(sourceId).get) // jMock is not up to the task
}

object JobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val FOLLOWS = 1

  val bob = 1L
  val mary = 23L
  val carl = 42L
  val jane = 56L
  val darcy = 62L

  val uuidGenerator = IdentityUuidGenerator
  var forwardingManager: ForwardingManager = null
  var shard1: Shard = null
  var shard2: Shard = null
  var shard3: Shard = null
  var shard4: Shard = null
  var lockingShard1: Shard = null
  var lockingShard2: Shard = null

  "Add" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      lockingShard1 = new FakeLockingShard(shard1)
      lockingShard2 = new FakeLockingShard(shard2)
    }

    "apply" in {
      "when the add takes effect" >> {
        val job = Add(bob, FOLLOWS, mary, 1, Time.now)

        expect {
          one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
          one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
          one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Normal, 1, Time.now))
          one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Normal, 1, Time.now))
          one(shard1).add(bob, mary, 1, Time.now)
          one(shard2).add(mary, bob, 1, Time.now)
        }

        job.apply((forwardingManager, uuidGenerator))
      }

      "when the add does not take effect" >> {
        "when the forward direction causes it to not take effect" >> {
          val job = Add(bob, FOLLOWS, mary, 1, Time.now)

          expect {
            one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
            one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
            one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Archived, 1, Time.now))
            one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Normal, 1, Time.now))
            one(shard1).archive(bob, mary, 1, Time.now)
            one(shard2).archive(mary, bob, 1, Time.now)
          }

          job.apply((forwardingManager, uuidGenerator))
        }

        "when the backward direction causes it to not take effect" >> {
          val job = Add(bob, FOLLOWS, mary, 1, Time.now)

          expect {
            one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
            one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
            one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Normal, 1, Time.now))
            one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Archived, 1, Time.now))
            one(shard1).archive(bob, mary, 1, Time.now)
            one(shard2).archive(mary, bob, 1, Time.now)
          }

          job.apply((forwardingManager, uuidGenerator))
        }
      }
    }

    "toJson" in {
      val job = Add(bob, FOLLOWS, mary, 1, Time.now)
      val json = job.toJson
      json mustMatch "Add"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"destination_id\":" + mary
      json mustMatch "\"updated_at\":" + Time.now
    }
  }

  "Remove" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      lockingShard1 = new FakeLockingShard(shard1)
      lockingShard2 = new FakeLockingShard(shard2)
    }

    "apply" in {
      "when the remove takes effect" >> {
        val job = new Remove(bob, FOLLOWS, mary, 1, Time.now)

        expect {
          one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
          one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
          one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Normal, 1, Time.now))
          one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Normal, 1, Time.now))
          one(shard1).remove(bob, mary, 1, Time.now)
          one(shard2).remove(mary, bob, 1, Time.now)
        }

        job.apply((forwardingManager, uuidGenerator))
      }
    }

    "toJson" in {
      val job = new Remove(bob, FOLLOWS, mary, 1, Time.now)
      val json = job.toJson
      json mustMatch "Remove"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"destination_id\":" + mary
      json mustMatch "\"updated_at\":" + Time.now
    }
  }

  "Archive" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      lockingShard1 = new FakeLockingShard(shard1)
      lockingShard2 = new FakeLockingShard(shard2)
    }

    "apply" in {
      "when the archive takes effect" >> {
        val time = System.currentTimeMillis
        val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now)

        expect {
          one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
          one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
          one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Normal, 1, Time.now))
          one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Normal, 1, Time.now))
          one(shard1).archive(bob, mary, 1, Time.now)
          one(shard2).archive(mary, bob, 1, Time.now)
        }

        job.apply((forwardingManager, uuidGenerator))
      }

      "when the archive does not take effect" >> {
        "when the forward direction causes it to not take effect" >> {
          val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now)

          expect {
            one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
            one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
            one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Removed, 1, Time.now))
            one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Normal, 1, Time.now))
            one(shard1).remove(bob, mary, 1, Time.now)
            one(shard2).remove(mary, bob, 1, Time.now)
          }

          job.apply((forwardingManager, uuidGenerator))
        }

        "when the backward direction causes it to not take effect" >> {
          val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now)

          expect {
            one(forwardingManager).find(bob, FOLLOWS, Direction.Forward) willReturn lockingShard1
            one(forwardingManager).find(mary, FOLLOWS, Direction.Backward) willReturn lockingShard2
            one(shard1).getMetadata(bob) willReturn Some(Metadata(bob, State.Normal, 1, Time.now))
            one(shard2).getMetadata(mary) willReturn Some(Metadata(mary, State.Removed, 1, Time.now))
            one(shard1).remove(bob, mary, 1, Time.now)
            one(shard2).remove(mary, bob, 1, Time.now)
          }

          job.apply((forwardingManager, uuidGenerator))
        }
      }
    }

    "toJson" in {
      val job = new jobs.single.Archive(bob, FOLLOWS, mary, 1, Time.now)
      val json = job.toJson
      json mustMatch "Archive"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"destination_id\":" + mary
      json mustMatch "\"updated_at\":" + Time.now
    }
  }

  "Archive" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      shard3 = mock[Shard]
      shard4 = mock[Shard]
    }

    "toJson" in {
      val job = new Archive(bob, FOLLOWS, Direction.Forward, Time.now, Priority.Low)
      val json = job.toJson
      json mustMatch "Archive"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"updated_at\":" + Time.now
      json mustMatch "\"priority\":" + Priority.Low.id
    }
  }

  "Unarchive" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      shard3 = mock[Shard]
      shard4 = mock[Shard]
    }

    "toJson" in {
      val job = new Unarchive(bob, FOLLOWS, Direction.Forward, Time.now, Priority.Low)
      val json = job.toJson
      json mustMatch "Unarchive"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"updated_at\":" + Time.now
      json mustMatch "\"priority\":" + Priority.Low.id
    }
  }

  "RemoveAll" should {
    doBefore {
      forwardingManager = mock[ForwardingManager]
      shard1 = mock[Shard]
      shard2 = mock[Shard]
      shard3 = mock[Shard]
      shard4 = mock[Shard]
    }

    "toJson" in {
      val job = RemoveAll(bob, FOLLOWS, Direction.Backward, Time.now, Priority.Low)
      val json = job.toJson
      json mustMatch "RemoveAll"
      json mustMatch "\"source_id\":" + bob
      json mustMatch "\"graph_id\":" + FOLLOWS
      json mustMatch "\"updated_at\":" + Time.now
      json mustMatch "\"priority\":" + Priority.Low.id
    }
  }
}
