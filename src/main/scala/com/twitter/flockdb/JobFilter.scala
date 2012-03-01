package com.twitter.flockdb

import com.twitter.logging.Logger
import com.twitter.common.base.Function
import com.twitter.common.zookeeper.{ZooKeeperClient, ZooKeeperMap, ZooKeeperUtils}
import com.twitter.gizzard.Stats
import java.util.{Set => JSet}
import org.apache.zookeeper.ZooDefs
import scala.collection.JavaConversions

trait JobFilter {
  /**
   * Returns true iff the job with the given sourceId, destID,
   * and graphId parameters should be executed.
   */
  def apply(sourceId: Long, graphId: Int, destId: Long): Boolean
}

/**
 * Default no-op filter. All operations pass.
 */
object NoOpFilter extends JobFilter {
  def apply(sourceId: Long, graphId: Int, destId: Long): Boolean = true
}

/**
 * A filter based on an independently-updated Java set.
 *
 * Set entries should have the form 'SOURCE_ID:GRAPH_ID:DEST_ID', each of
 * which specifies either a user ID or a wildcard (*).
 * A wildcard may be specified for at most one of SOURCE_ID and DEST_ID.
 *
 * Ex. Filter all writes from user 1234 on graph 5: "1234:5:*".
 * Filter all writes to user 2345 on all graphs: "*:*:2345".
 * Filter writes on edges between users 100 and 200 on all graphs: "100:*:200".
 */
class SetFilter(set: JSet[String]) extends JobFilter {
  private val WILDCARD = "*"
  private val FILTERS = Seq(
    (true, true, true),
    (true, false, true),
    (true, true, false),
    (false, true, true),
    (false, false, true),
    (true, false, false))

  Stats.addGauge("active-filters") { set.size() }

  def apply(sourceId: Long, graphId: Int, destId: Long): Boolean = {
    def filterKey(filter: (Boolean, Boolean, Boolean)) = {
      "%s:%s:%s".format(
          if (filter._1) sourceId.toString else WILDCARD,
          if (filter._2) graphId.toString else WILDCARD,
          if (filter._3) destId.toString else WILDCARD
      )
    }

    for (filter <- FILTERS) {
      if (set.contains(filterKey(filter))) {
        Stats.incr("edges-filtered")
        return false
      }
    }
    Stats.incr("edges-passed")
    true
  }
}

/**
 * A filter based on a ZooKeeperMap. Filters are stored as node keys under the given zkPath.
 * The Zookeeper node values are not used.
 */
class ZooKeeperSetFilter(zkClient: ZooKeeperClient, zkPath: String) extends JobFilter {
  val log = Logger.get

  private val deserializer = new Function[Array[Byte], Unit]() {
    override def apply(data : Array[Byte]) = ()
  }

  ZooKeeperUtils.ensurePath(zkClient, ZooDefs.Ids.OPEN_ACL_UNSAFE, zkPath)
  private val zkMap: ZooKeeperMap[Unit] = ZooKeeperMap.create(zkClient, zkPath, deserializer)
  log.info("Initial filter set: " + ( JavaConversions.asScalaSet(zkMap.keySet()).mkString(", ")))

  private val setFilter = new SetFilter(zkMap.keySet())

  def apply(sourceId: Long, graphId: Int, destId: Long): Boolean =
    setFilter(sourceId, graphId, destId)
}
