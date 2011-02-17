/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.flockdb

abstract class State(val id: Int, val name: String, val ordinal: Int) extends Ordered[State] {
  def max(other: State) = if (this > other) this else other
  def compare(s: State) = ordinal.compare(s.ordinal)
}

object State {
  def apply(id: Int) = id match {
    case Normal.id => Normal
    case Removed.id => Removed
    case Archived.id => Archived
    case Negative.id => Negative
  }

  case object Normal extends State(0, "Normal", 0)
  case object Negative extends State(3, "Negative", 1)
  case object Removed extends State(1, "Removed", 3)
  case object Archived extends State(2, "Archived", 2)
}
