namespace java com.twitter.service.flock.thrift
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
