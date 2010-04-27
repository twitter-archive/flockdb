namespace java com.twitter.flockdb.thrift
namespace rb Flock

exception FlockException {
  1: string description
}

struct Results {
  # byte-packed list of i64, little-endian:
  1: binary ids
  2: i64 next_cursor
  3: i64 prev_cursor
}

struct Page {
  1: i32 count
  2: i64 cursor
}

struct Edge {
  1: i64 source_id
  2: i64 destination_id
  3: i64 position
  4: i32 updated_at
  5: i32 count
  6: i32 state_id
}

enum SelectOperationType {
  SimpleQuery = 1
  Intersection = 2
  Union = 3
  Difference = 4
}

# Add and Negate are both "normal" states. you can use them to track 2 different "flavors" of existing edge.
# Remove is an edge which no longer exists.
# Archive is an edge which refers to a node that's been removed.
enum ExecuteOperationType {
  Add = 1
  Remove = 2
  Archive = 3
  Negate = 4
}

enum EdgeState {
  Positive = 0
  Negative = 3
  Removed = 1
  Archived = 2
}

# Basic FlockDB query term.
# Terms can query a specific edge in either direction, or a one-to-many edge in either direction.
# Wildcard queries can be specified by leaving `destination_ids` empty.
# Only edges matching the set of given `state_ids` are included.
#
# Examples:
#   (30, 2, true, [40], [Positive])
#       -- in graph 2, is there an edge from 30 -> 40?
#   (30, 1, false, [40, 50, 60], [Positive])
#       -- in graph 1, which of (40 -> 30, 50 -> 30, 60 -> 30) exist?
#   (30, 3, false, [], [Removed, Archived])
#       -- in graph 3, which edges point to -> 30, and are either removed or archived?
struct QueryTerm {
  1: i64 source_id
  2: i32 graph_id
  3: bool is_forward
  # byte-packed list of i64, little-endian. if not present, it means "all":
  4: optional binary destination_ids
  5: optional list<i32> state_ids
}

struct SelectOperation {
  1: SelectOperationType operation_type
  2: optional QueryTerm term
}

enum Priority {
  Low = 1
  Medium = 2
  High = 3
}

struct ExecuteOperation {
  1: ExecuteOperationType operation_type
  2: QueryTerm term
  3: optional i64 position
}

struct ExecuteOperations {
  1: list<ExecuteOperation> operations
  2: optional i32 execute_at
  3: Priority priority
}

struct SelectQuery {
  1: list<SelectOperation> operations
  2: Page page
}

struct EdgeQuery {
  1: QueryTerm term
  2: Page page
}

struct EdgeResults {
  1: list<Edge> edges
  2: i64 next_cursor
  3: i64 prev_cursor
}

service FlockDB {
  // return true if the edge exists.
  bool contains(1: i64 source_id, 2: i32 graph_id, 3: i64 destination_id) throws(1: FlockException ex)
  
  // return all data about an edge if it exists (otherwise, throw an exception).
  Edge get(1: i64 source_id, 2: i32 graph_id, 3: i64 destination_id) throws(1: FlockException ex)

  // perform a list of queries in parallel. each query may be paged, and may be compound.
  list<Results> select2(1: list<SelectQuery> queries) throws(1: FlockException ex)

  // perform a list of queries in parallel, and return the count 
  binary count2(1: list<list<SelectOperation>> queries) throws(1: FlockException ex)
  list<EdgeResults> select_edges(1: list<EdgeQuery> queries) throws(1: FlockException ex)
  void execute(1: ExecuteOperations operations) throws(1: FlockException ex)

  // deprecated:
  binary counts_of_destinations_for(1: binary source_ids, 2: i32 graph_id) throws(1: FlockException ex)
  binary counts_of_sources_for(1: binary destination_ids, 2: i32 graph_id) throws(1: FlockException ex)
  i32 count(1: list<SelectOperation> operations)
  Results select(1: list<SelectOperation> operations, 2: Page page)
}
