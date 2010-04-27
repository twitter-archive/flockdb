#!/usr/bin/env ruby
#
# Copyright 2010 Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

$:.push(File.dirname($0))
require 'optparse'
require 'socket'
require 'simple'

PACKAGE = "com.twitter.service.flock.edges"

GRAPH_COUNT = 15

SHARD_CLASS = {
  :read_only => "ReadOnlyShard",
  :blocked => "BlockedShard",
  :write_only => "WriteOnlyShard",
  :black_hole => "BlackHoleShard",
  :replica => "ReplicatingShard"
}

SHARD_NAME = Hash[*SHARD_CLASS.map { |a, b| [ b, a ] }.flatten]

DIRECTIONS = [ "backward", "forward" ]

ShardInfo = ThriftClient::Simple.make_struct(:ShardInfo,
  ThriftClient::Simple::Field.new(:class_name, ThriftClient::Simple::STRING, 1),
  ThriftClient::Simple::Field.new(:table_prefix, ThriftClient::Simple::STRING, 2),
  ThriftClient::Simple::Field.new(:hostname, ThriftClient::Simple::STRING, 3),
  ThriftClient::Simple::Field.new(:source_type, ThriftClient::Simple::STRING, 4),
  ThriftClient::Simple::Field.new(:destination_type, ThriftClient::Simple::STRING, 5),
  ThriftClient::Simple::Field.new(:busy, ThriftClient::Simple::I32, 6),
  ThriftClient::Simple::Field.new(:shard_id, ThriftClient::Simple::I32, 7))

ShardChild = ThriftClient::Simple.make_struct(:ShardChild,
  ThriftClient::Simple::Field.new(:shard_id, ThriftClient::Simple::I32, 1),
  ThriftClient::Simple::Field.new(:weight, ThriftClient::Simple::I32, 2))

Forwarding = ThriftClient::Simple.make_struct(:Forwarding,
  ThriftClient::Simple::Field.new(:table_id, ThriftClient::Simple::I32, 1),
  ThriftClient::Simple::Field.new(:base_id, ThriftClient::Simple::I64, 2),
  ThriftClient::Simple::Field.new(:shard_id, ThriftClient::Simple::I32, 3))

ShardMigration = ThriftClient::Simple.make_struct(:ShardMigration,
  ThriftClient::Simple::Field.new(:source_shard_id, ThriftClient::Simple::I32, 1),
  ThriftClient::Simple::Field.new(:destination_shard_id, ThriftClient::Simple::I32, 2),
  ThriftClient::Simple::Field.new(:replicating_shard_id, ThriftClient::Simple::I32, 3),
  ThriftClient::Simple::Field.new(:write_only_shard_id, ThriftClient::Simple::I32, 4))

class ShardManager < ThriftClient::Simple::ThriftService
  thrift_method :create_shard, i32, field(:shard, struct(ShardInfo), 1)
  thrift_method :find_shard, i32, field(:shard, struct(ShardInfo), 1)
  thrift_method :get_shard, struct(ShardInfo), field(:shard_id, i32, 1)
  thrift_method :update_shard, void, field(:shard, struct(ShardInfo), 1)
  thrift_method :delete_shard, void, field(:shard_id, i32, 1)

  thrift_method :add_child_shard, void, field(:parent_shard_id, i32, 1), field(:child_shard_id, i32, 2), field(:weight, i32, 3)
  thrift_method :remove_child_shard, void, field(:parent_shard_id, i32, 1), field(:child_shard_id, i32, 2)
  thrift_method :replace_child_shard, void, field(:old_child_shard_id, i32, 1), field(:new_child_shard_id, i32, 2)
  thrift_method :list_shard_children, list(struct(ShardChild)), field(:shard_id, i32, 1)

  thrift_method :mark_shard_busy, void, field(:shard_id, i32, 1), field(:busy, i32, 2)
  thrift_method :copy_shard, void, field(:source_shard_id, i32, 1), field(:destination_shard_id, i32, 2)
  thrift_method :setup_migration, struct(ShardMigration), field(:source_shard_info, struct(ShardInfo), 1), field(:destination_shard_info, struct(ShardInfo), 2)
  thrift_method :migrate_shard, void, field(:migration, struct(ShardMigration), 1)

  thrift_method :set_forwarding, void, field(:forwarding, struct(Forwarding), 1)
  thrift_method :replace_forwarding, void, field(:old_shard_id, i32, 1), field(:new_shard_id, i32, 2)
  thrift_method :get_forwarding, struct(ShardInfo), field(:table_id, i32, 1), field(:base_id, i64, 2)
  thrift_method :get_forwarding_for_shard, struct(Forwarding), field(:shard_id, i32, 1)
  thrift_method :get_forwardings, list(struct(Forwarding))
  thrift_method :reload_forwardings, void
  thrift_method :find_current_forwarding, struct(ShardInfo), field(:table_id, i32, 1), field(:id, i64, 2)

  thrift_method :shard_ids_for_hostname, list(i32), field(:hostname, string, 1), field(:class_name, string, 2)
  thrift_method :shards_for_hostname, list(struct(ShardInfo)), field(:hostname, string, 1), field(:class_name, string, 2)
  thrift_method :get_busy_shards, list(struct(ShardInfo))
  thrift_method :get_parent_shard, struct(ShardInfo), field(:shard_id, i32, 1)
  thrift_method :get_root_shard, struct(ShardInfo), field(:shard_id, i32, 1)
  thrift_method :get_child_shards_of_class, list(struct(ShardInfo)), field(:parent_shard_id, i32, 1), field(:class_name, string, 2)

  thrift_method :rebuild_schema, void
end

class JobManager < ThriftClient::Simple::ThriftService
  thrift_method :retry_errors, void
  thrift_method :stop_writes, void
  thrift_method :resume_writes, void
  thrift_method :retry_errors_for, void, field(:priority, i32, 1)
  thrift_method :stop_writes_for, void, field(:priority, i32, 1)
  thrift_method :resume_writes_for, void, field(:priority, i32, 1)
  thrift_method :is_writing, bool, field(:priority, i32, 1)
  thrift_method :inject_job, void, field(:priority, i32, 1), field(:job, string, 2)
end

SHARD_PORT = 7917
JOB_PORT = 7919

# for use inside flocker:
Shard = Struct.new("Shard", :id, :type, :faction, :hostname, :table_prefix, :busy, :children, :weight)


# ruby 1.9 is incompatible with 1.8 :(
def ord(char)
  char.ord
rescue
  char[0]
end

def confirm(message, &block)
  return if $options[:force]
  print "#{message} [Y] "
  STDOUT.flush
  if [ "y", "Y", "" ].include?(STDIN.gets.chomp)
    return
  else
    yield if block
    exit 1
  end
end

def get_shard(shard_info, weight=1)
  class_bits = shard_info.class_name.split(".")
  shard_type, faction = class_bits[-1], class_bits[-2]

  shard = Shard.new(shard_info.shard_id, shard_type, faction, shard_info.hostname, shard_info.table_prefix, shard_info.busy)
  shard.children = $service.list_shard_children(shard_info.shard_id).map { |child| get_shard($service.get_shard(child.shard_id), child.weight) }
  shard.weight = weight
  shard
end

def pp_children(shard)
  shard.children.map do |s|
    pp_shard(s) + (s.weight == 1 ? "" : "<#{s.weight}>")
  end.join(', ')
end

def pp_shard(shard)
  ($options[:show_ids] ? "[#{shard.id}]" : "") + if shard.type == "SqlShard"
    "#{shard.hostname}/#{shard.table_prefix}" + (shard.busy > 0 ? " (BUSY)" : "")
  else
    "#{shard.faction}-#{SHARD_NAME[shard.type]}(#{pp_children(shard)})"
  end
end

def with_each_shard_for_db(database_name, options={}, &block)
  shards = $service.shards_for_hostname(database_name, "#{PACKAGE}.SqlShard")
  if shards.size == 0
    puts "No shards are using '#{database_name}'!"
    return
  end
  puts "Shards on #{database_name}: #{shards.size}" if options[:dots]
  count = 0
  shards.each do |shard|
    yield shard
    if options[:dots]
      count += 1
      STDOUT.print "." if count % 10 == 0
      STDOUT.print "(#{count})" if count % 100 == 0
      STDOUT.flush
    end
  end
  puts "done." if options[:dots]
end

def connect_shard_service(hostname)
  ShardManager.new(TCPSocket.new(hostname, SHARD_PORT))
end

def connect_job_service(hostname)
  JobManager.new(TCPSocket.new(hostname, JOB_PORT))
end

def parent_of_type(shard, shard_type)
  wanted_class = SHARD_CLASS[shard_type]
  parent = $service.get_parent_shard(shard.shard_id)
  while !parent.class_name.include?(wanted_class) && parent.shard_id != shard.shard_id
    shard = parent
    parent = $service.get_parent_shard(shard.shard_id)
  end
  if parent.class_name.include?(wanted_class)
    [ parent, shard ]
  else
    nil
  end
end

# inject a shard above a given one, for the purpose of making it readonly, etc
def inject_shard(shard_type, shard)
  shard = $service.get_shard(shard) if shard.instance_of?(Fixnum)
  new_shard = ShardInfo.new("#{PACKAGE}.#{SHARD_CLASS[shard_type]}",
                            "#{shard.table_prefix}_#{shard_type}_inject",
                            shard.hostname, "", "")
  new_shard_id = $service.create_shard(new_shard)
  parent_shard = $service.get_parent_shard(shard.shard_id)
  if parent_shard.shard_id != shard.shard_id
    child_info = $service.list_shard_children(parent_shard.shard_id).select do |child|
      child.shard_id == shard.shard_id
    end.first
    $service.remove_child_shard(parent_shard.shard_id, shard.shard_id)
    $service.add_child_shard(parent_shard.shard_id, new_shard_id, child_info.weight)
  else
    forwarding = $service.get_forwarding_for_shard(shard.shard_id)
    if forwarding.nil?
      puts "*** Orphan shard: #{shard.inspect}"
    else
      forwarding.shard_id = new_shard_id
      $service.set_forwarding(forwarding)
    end
  end
  $service.add_child_shard(new_shard_id, shard.shard_id, $options[:weight])
end

def uninject_shard(shard)
  shard = $service.get_shard(shard) if shard.instance_of?(Fixnum)
  children = $service.list_shard_children(shard.shard_id)
  children.each do |child|
    $service.remove_child_shard(shard.shard_id, child.shard_id)
  end
  parent_shard = $service.get_parent_shard(shard.shard_id)
  if parent_shard.shard_id != shard.shard_id
    $service.remove_child_shard(parent_shard.shard_id, shard.shard_id)
    children.each do |child|
      $service.add_child_shard(parent_shard.shard_id, child.shard_id, child.weight)
    end
  else
    forwarding = $service.get_forwarding_for_shard(shard.shard_id)
    if forwarding.nil?
      puts "*** Orphan shard: #{shard.inspect}"
    else
      forwarding.shard_id = children.first.shard_id
      $service.set_forwarding(forwarding)
    end
  end
  $service.delete_shard(shard.shard_id)
end

def setup_one_migration(source_shard_id, hostname)
  source_shard_info = $service.get_shard(source_shard_id)
  destination_shard_info = source_shard_info.dup
  destination_shard_info.hostname = hostname
  destination_shard_info.table_prefix += $options[:append_table_name]
  migration = $service.setup_migration(source_shard_info, destination_shard_info)
  puts "Setup migration: shard #{migration.source_shard_id} -> #{migration.destination_shard_id}"
  migration
end

def perform_migrations(migrating_shards, destination_db_hostname)
  puts "Migrating #{migrating_shards.size} shards."
  migrating_shards.each do |shard|
    puts "  " + pp_shard(get_shard(shard))
  end
  confirm("Is that okay?")

  puts
  migrations = migrating_shards.map do |shard|
    setup_one_migration(shard.shard_id, destination_db_hostname)
  end
  confirm("Ready to push new shard config to flapps (#{$options[:flapps].join(', ')})?")
  $options[:flapps].each do |hostname|
    puts "Reloading forwardings on #{hostname}"
    connect_shard_service(hostname).reload_forwardings()
  end
  confirm("Ready to start the migration?")

  puts
  migrations.each_with_index do |migration, i|
    server_hostname = $options[:flapps][i % $options[:flapps].size]
    puts "Migrating shard #{migration.source_shard_id} on #{server_hostname}"
    connect_shard_service(server_hostname).migrate_shard(migration)
  end
  puts
  puts "Migration underway!"
  puts
  show_busy
  puts
end

def find_migration(destination_shard_id, need_busy=true)
  destination_shard = $service.get_shard(destination_shard_id)
  unless destination_shard.busy > 0 or !need_busy
    puts "Shard #{destination_shard_id} is not busy."
    exit 1
  end
  write_only_shard = $service.get_parent_shard(destination_shard_id)
  unless write_only_shard.class_name.include?("WriteOnly")
    puts "Shard #{write_only_shard.id} is not a write-only shard."
    exit 1
  end
  replica_shard = $service.get_parent_shard(write_only_shard.shard_id)
  unless replica_shard.class_name.include?("Replica")
    puts "Shard #{replica_shard.id} is not a replica."
    exit 1
  end
  children = $service.list_shard_children(replica_shard.shard_id).select { |child| child.shard_id != write_only_shard.shard_id }
  unless children.size == 1
    puts "Should only be one source shard...?"
    exit 1
  end
  source_shard_id = children[0].shard_id
  ShardMigration.new(source_shard_id, destination_shard_id, replica_shard.shard_id, write_only_shard.shard_id)
end

def make_shard(hostnames, lower_bound, num)
  table_ids = []
  table_names = []
  column_type = "INT UNSIGNED"
  $options[:graphs].each do |graph_id|
    [ 0, 1 ].each do |direction|
      table_ids << (direction == 1 ? graph_id : -graph_id)
      table_names << "edges_#{DIRECTIONS[direction]}_#{graph_id}_%03d" % num
    end
  end

  table_ids.zip(table_names).each do |table_id, table_name|
    distinct = "@"
    shard_id_set = hostnames.map do |hostname|
      distinct = (ord(distinct) + 1).chr
      shard = ShardInfo.new("#{PACKAGE}.SqlShard", "#{table_name}_#{distinct}", hostname, column_type, column_type, 0, 0)
      $service.create_shard(shard)
    end
    replica_shard = ShardInfo.new("#{PACKAGE}.#{SHARD_CLASS[:replica]}", "#{table_name}_replicating", "localhost", "", "", 0, 0)
    replica_id = $service.create_shard(replica_shard)
    shard_id_set.each_with_index { |shard_id, n| $service.add_child_shard(replica_id, shard_id, (n == 0) ? $options[:weight] : 1) }

    $service.set_forwarding(Forwarding.new(table_id, lower_bound, replica_id))
  end
end


## ----- commands:

def show_forwardings
  puts "GRAPH  BASE_USER_ID    SHARD"
  $service.get_forwardings().each do |forwarding|
    if $options[:graphs] and !$options[:graphs].include?(forwarding.table_id) and !$options[:graphs].include?(-forwarding.table_id)
      next
    end
    shard = $service.get_shard(forwarding.shard_id)
    is_forward = forwarding.table_id > 0
    puts "%3d %015x %s %s" % [forwarding.table_id.abs, forwarding.base_id, is_forward ? "->" : "<-", pp_shard(get_shard(shard)) ]
  end
end

def show_busy
  busy = $service.get_busy_shards
  busy.each do |shard|
    puts "Busy: #{pp_shard(get_shard(shard))}"
  end
  puts "No busy shards." if busy.empty?
end

def find(user_id)
  $options[:graphs].each do |graph_id|
    forward_si = $service.find_current_forwarding(graph_id, user_id)
    backward_si = $service.find_current_forwarding(-graph_id, user_id)
    puts "User_id #{user_id}, graph #{graph_id}"
    puts "  Forward: #{pp_shard(get_shard(forward_si))}"
    puts "  Backward: #{pp_shard(get_shard(backward_si))}"
  end
end

def dump_db(database_name)
  with_each_shard_for_db(database_name) do |shard|
    puts pp_shard(get_shard($service.get_root_shard(shard.shard_id)))
  end
end

def swap_db(old_database, new_database)
  with_each_shard_for_db(old_database, :dots => true) do |shard|
    shard.hostname = new_database
    $service.update_shard(shard)
  end
end

def rotate_db(database_name)
  missing = 0
  with_each_shard_for_db(database_name, :dots => true) do |shard|
    replica, db_shard = parent_of_type(shard, :replica)
    if replica
      children = $service.list_shard_children(replica.shard_id)
      old_master = children[0]
      rotating_child_weight = nil
      children.each do |child|
        $service.remove_child_shard(replica.shard_id, child.shard_id)
        if child.shard_id == db_shard.shard_id
          rotating_child_weight = child.weight
          child.weight = old_master.weight
        end
      end
      old_master.weight = rotating_child_weight
      children.each do |child|
        $service.add_child_shard(replica.shard_id, child.shard_id, child.weight)
      end
    else
      missing += 1
    end
  end
  puts "#{missing} shard(s) were NOT rotated because they're not replicating." if missing > 0
end

def weight_db(database_name)
  missing = 0
  with_each_shard_for_db(database_name, :dots => true) do |shard|
    replica, db_shard = parent_of_type(shard, :replica)
    if replica
      $service.list_shard_children(replica.shard_id).select { |child| child.shard_id == db_shard.shard_id }.each do |child|
        $service.remove_child_shard(replica.shard_id, child.shard_id)
        $service.add_child_shard(replica.shard_id, child.shard_id, $options[:weight])
      end
    else
      missing += 1
    end
  end
  puts "#{missing} shard(s) were NOT re-weigted because they're not replicating." if missing > 0
end

def add_replica_db(current_database_name, new_database_name)
  missing = 0
  with_each_shard_for_db(current_database_name, :dots => true) do |shard|
    replica, _ = parent_of_type(shard, :replica)
    if replica
      children = $service.list_shard_children(replica.shard_id)
      new_shard = ShardInfo.new(shard.class_name, shard.table_prefix, new_database_name,
                                shard.source_type, shard.destination_type)
      new_shard_id = $service.create_shard(new_shard)
      $service.add_child_shard(replica.shard_id, new_shard_id, $options[:weight])
    else
      missing += 1
    end
  end
  puts "#{missing} shard(s) were NOT added because they're not replicating." if missing > 0
end

def remove_replica_db(database_name)
  missing = 0
  with_each_shard_for_db(database_name, :dots => true) do |shard|
    replica, db_shard = parent_of_type(shard, :replica)
    if replica
      children = $service.list_shard_children(replica.shard_id)
      $service.remove_child_shard(replica.shard_id, db_shard.shard_id)
      $service.delete_shard(db_shard.shard_id)
    else
      missing += 1
    end
  end
  puts "#{missing} shard(s) were NOT removed because they're not replicating." if missing > 0
end

def set_db(database_name, shard_type)
  missing = 0
  with_each_shard_for_db(database_name, :dots => true) do |shard|
    parent, _ = parent_of_type(shard, shard_type)
    if parent
      missing += 1
    else
      inject_shard(shard_type, shard.shard_id)
    end
  end
  puts "#{missing} shard(s) were ALREADY in state #{shard_type}." if missing > 0
end

def unset_db(database_name, shard_type)
  missing = 0
  with_each_shard_for_db(database_name, :dots => true) do |shard|
    undesired, _ = parent_of_type(shard, shard_type)
    if undesired
      uninject_shard(undesired.shard_id)
    else
      missing += 1
    end
  end
  puts "#{missing} shard(s) were NOT in state #{shard_type}." if missing > 0
end

def set_db_for(shard_id, shard_type)
  shard = $service.get_shard(shard_id)
  parent, _ = parent_of_type(shard, shard_type)
  if parent
    puts "Already in that state: #{pp_shard(get_shard(shard))}."
  else
    puts "Changing #{pp_shard(get_shard(shard))}"
    inject_shard(shard_type, shard.shard_id)
  end
end

def unset_db_for(shard_id, shard_type)
  shard = $service.get_shard(shard_id)
  undesired, _ = parent_of_type(shard, shard_type)
  if undesired
    puts "Changing #{pp_shard(get_shard(shard))}"
    uninject_shard(undesired.shard_id)
  else
    puts "Not in that state: #{pp_shard(get_shard(shard))}"
  end
end

def migrate(source_db_hostname, destination_db_hostname)
  shards = $service.shards_for_hostname(source_db_hostname, "#{PACKAGE}.SqlShard")
  # bucket by table_id so we get the same percentage of each.
  forwardings = Hash.new { |h, k| h[k] = [] }
  active_migrations = 0
  shards.each do |shard|
    root_shard = $service.get_root_shard(shard.shard_id)
    forwarding = $service.get_forwarding_for_shard(root_shard.shard_id)
    if forwarding
      # make sure there's not an outstanding migration.
      if $service.get_child_shards_of_class(root_shard.shard_id, "#{PACKAGE}.SqlShard").any? { |s| s.busy != 0 }
        active_migrations += 1
      else
        if $options[:graph] and $options[:graph].include?(forwarding.table_id.abs)
          # skip
        else
          forwardings[forwarding.table_id] << shard
        end
      end
    end
  end
  total_shards = forwardings.inject(0) { |sum, kv| sum + kv[1].size }
  puts "There are #{active_migrations} migrations in progress on #{source_db_hostname}." if active_migrations > 0
  puts "There are #{total_shards} active shards using #{source_db_hostname}."
  migrating_shards = forwardings.map { |k, v| v.slice(0, $options[:percent] / 100.0 * v.size) }.flatten.compact
  perform_migrations(migrating_shards, destination_db_hostname)
end

def migrate_slaves(master_db_hostname, destination_db_hostname)
  shards = $service.shards_for_hostname(master_db_hostname, "#{PACKAGE}.SqlShard")
  active_shards = []
  active_migrations = 0
  shards.each do |shard|
    root = $service.get_root_shard(shard.shard_id)
    forwarding = $service.get_forwarding_for_shard(root.shard_id)
    if forwarding
      sql_children = $service.get_child_shards_of_class(root.shard_id, "#{PACKAGE}.SqlShard")
      # make sure there's not an outstanding migration.
      if sql_children.any? { |s| s.busy != 0 }
        active_migrations += 1
      else
        active_shards << sql_children.select { |s| ![master_db_hostname, destination_db_hostname].include?(s.hostname) }.first
      end
    end
  end
  active_shards.reject! { |s| s.nil? }
  old_shard_hostnames = active_shards.map { |s| s.hostname }.sort.uniq
  puts "There are #{active_migrations} migrations in progress on #{master_db_hostname}." if active_migrations > 0
  if active_shards.size == 0
    puts "All slave shards of #{master_db_hostname} are using #{destination_db_hostname}."
  else
    puts "There are #{active_shards.size} active slave shards using #{old_shard_hostnames.join(", ")}."
    migrating_shards = active_shards.slice(0, $options[:percent] / 100.0 * active_shards.size)
    perform_migrations(migrating_shards, destination_db_hostname)
  end
end

def migrate_one(source_shard_id, hostname)
  migration = setup_one_migration(source_shard_id, hostname)
  confirm("Ready to push new shard config to flapps (#{$options[:flapps].join(', ')})?") do
    unmigrate(migration.destination_shard_id)
  end
  $options[:flapps].each do |hostname|
    puts "Reloading forwardings on #{hostname}"
    connect_shard_service(hostname).reload_forwardings()
  end
  confirm("Ready to start the migration?")
  $service.migrate_shard(migration)
  puts "Migration started!"
  show_busy
end

def copy_one(source_shard_id, hostname)
  destination_shard_id = hostname.to_i
  created_shard = false
  if destination_shard_id == 0
    source_shard = $service.get_shard(source_shard_id)
    destination_shard = source_shard.dup
    destination_shard.hostname = hostname
    destination_shard.table_prefix += $options[:append_table_name]
    destination_shard_id = $service.create_shard(destination_shard)
    created_shard = true
  end
  puts "Setup copy: shard #{source_shard_id} -> #{destination_shard_id}"

  if created_shard
    confirm("Ready to push new shard config to flapps (#{$options[:flapps].join(', ')})?") do
      $service.delete_shard(destination_shard_id)
    end
    $options[:flapps].each do |hostname|
      puts "Reloading forwardings on #{hostname}"
      connect_shard_service(hostname).reload_forwardings()
    end
  end
  confirm("Ready to start the copy?")
  $service.copy_shard(source_shard_id, destination_shard_id)
  puts "Copying shard #{source_shard_id} -> #{destination_shard_id}"
  show_busy
end

def setup_dev_shards
  (1..GRAPH_COUNT).each do |graph_id|
    shards = [ "forward", "backward" ].map do |direction|
      $service.create_shard(ShardInfo.new("#{PACKAGE}.SqlShard", "#{direction}_#{graph_id}", "localhost", "INT UNSIGNED", "INT UNSIGNED", 0))
    end
    $service.set_forwarding(Forwarding.new(graph_id, 0, shards[0]))
    $service.set_forwarding(Forwarding.new(-graph_id, 0, shards[1]))
  end

  $service.reload_forwardings()
end

# hostname_lists is a list of lists: #replicas x [ shard_hosts ]
def make_shards(num_shards, hostname_lists)
  puts "Creating shards..."
  num_shards.times do |i|
    shard_hostnames = hostname_lists.map { |h| h[i % h.size] }
    lower_bound = (1 << 60) / num_shards * i
    make_shard(shard_hostnames, lower_bound, i)
    print "."
    print "#{i+1}" if (i + 1) % 100 == 0
    STDOUT.flush
  end
  puts "Done."
end

def start_all_writes
  $options[:flapps].each do |hostname|
    connect_job_service(hostname).resume_writes
    puts "#{hostname}"
  end
end

def stop_all_writes
  $options[:flapps].each do |hostname|
    connect_job_service(hostname).stop_writes
    puts "#{hostname}"
  end
end

def start_writes(priority)
  $options[:flapps].each do |hostname|
    connect_job_service(hostname).resume_writes_for(priority)
    puts "#{hostname}"
  end
end

def stop_writes(priority)
  $options[:flapps].each do |hostname|
    connect_job_service(hostname).stop_writes_for(priority)
    puts "#{hostname}"
  end
end

def check_writes
  $options[:flapps].each do |hostname|
    service = connect_job_service(hostname)
    status = service.is_writing(1), service.is_writing(2), service.is_writing(3)
    puts "#{hostname}: " + [0, 1, 2].map { |s| "#{s + 1}=#{status[s] ? 'on' : 'off'}" }.join(" ")
  end
end

def reload_forwardings
  $options[:flapps].each do |hostname|
    puts "Reloading forwardings on #{hostname}"
    connect_shard_service(hostname).reload_forwardings()
  end
end

def flush_queues
  $options[:flapps].each do |hostname|
    puts "Flushing all error queues on #{hostname}"
    connect_job_service(hostname).retry_errors()
  end
end

def flush_queues_for(priority)
  $options[:flapps].each do |hostname|
    puts "Flushing queue #{priority} on #{hostname}"
    connect_job_service(hostname).retry_errors_for(priority)
  end
end

def kick_migration(destination_shard_id)
  migration = find_migration(destination_shard_id)
  puts "Migration: #{migration}"
  confirm("Ready to kick off that migration [again]?")
  $service.migrate_shard(migration)
  puts "Doing!"
end

def unmigrate(shard_id)
  migration = find_migration(shard_id, false)
  puts "Cancelling migration: #{migration}"
  $service.remove_child_shard(migration.replicating_shard_id, migration.source_shard_id)
  $service.remove_child_shard(migration.replicating_shard_id, migration.write_only_shard_id)
  $service.remove_child_shard(migration.write_only_shard_id, migration.destination_shard_id)
  $service.replace_child_shard(migration.replicating_shard_id, migration.source_shard_id)
  $service.replace_forwarding(migration.replicating_shard_id, migration.source_shard_id)
  $service.delete_shard(migration.destination_shard_id)
  $service.delete_shard(migration.write_only_shard_id)
  $service.delete_shard(migration.replicating_shard_id)
end


## ----- cli

def usage
  puts <<-USAGE
Commands:

    --- SHARDS ---
    show
        show current forwarding tables
    busy
        show shards that are currently migrating/copying
    find <user-id>
        find the shard for a user and display its topology
        (requires --graph)
    dump-db <db-hostname>
        dump a list of all forwardings that refer to a database
    swap-db <old-db-hostname> <new-db-hostname>
        replace all shards pointing to one db with a new db
    rotate-db <db-hostname>
        move a db to the front (primary) of any replicating shards it's in
    weight-db <db-hostname>
        rebalance replica shards on the given db to have a new weight
    add-replica-db <current-db-hostname> <new-db-hostname>
        add a db to the replica list that an existing db is servicing
    remove-replica-db <db-hostname>
        remove a db from its replica list
    set-db <db-hostname> <shard-type>
        alter the shards on a database to add a filter
    unset-db <db-hostname> <shard-type>
        remove a filter from the shards on a database
    set-db-for <shard-id> <shard-type>
        alter a single shard to add a filter
    unset-db-for <shard-id> <shard-type>
        remove a filter from a single shard
    migrate <current-db-hostname> <new-db-hostname>
        migrate (a percentage) of shards from one db to another
    migrate-slaves <master-db-hostname> <slave-db-hostname>
        migrate (a percentage) of shards that are slaves to shards on the
        master db but are hosted somewhere else (in other words, rectify the
        slave shards to match the master/slave pairing listed)
    migrate-one <shard-id> <new-db-hostname>
        migrate one a specific shard to a new database
    copy-one <shard-id> <shard-id|db-hostname>
        copy an existing shard to another shard (possibly creating the new
        shard if db-hostname is provided)
    setup-dev
        create shards for graphs 1-10, for use in development
    mkshards <count> <host-lists>
        build N replicating shards (see below for host-lists format; requires
        --graph)

    --- WRITE QUEUES ---
    start-all-writes
        start/resume writing at all priorities
    stop-all-writes
        stop/pause writing at all priorities
    start-writes <priority>
        start/resume writing at priority (1-3)
    stop-writes <priority>
        stop/pause writing at priority (1-3)
    check-writes
        display on/off state for all write queues on the listed flapps
    push
        tell flapp servers to reload the forwarding/shard tables
    flush-all
        tell flapp servers to replay the error queue (postponed writes)
    flush <priority>
        tell flapp servers to replay any error queue migrations
    inject <priority> <job>
        inject a job (as json) into the work queue of the given priority
USAGE
  common_usage
end

def common_usage
  puts
  puts "Shard types are: #{SHARD_CLASS.keys.join(', ')}"
  puts
  puts "Host lists for mkshards are lists of hostnames separated by ';', with "
  puts "individual hostnames separated by ','. For example:"
  puts "    host1,host2,host3;host4,host5,host6"
  puts "creates a set of 3 shards with 2 replicas each: (host1 + host4), "
  puts "    (host2 + host5), (host3 + host6)."
  puts
end

def parse_args
  options = {
    :flapps => [ "flapp001" ],
    :nameserver => "flockdb001",
    :database_name => "flock_production",
    :append_table_name => "",
    :percent => 100,
    :weight => 1,
  }

  # let ~/.flapps be a default set of flapp servers
  begin
    options[:flapps] = File.open("#{ENV['HOME']}/.flapps").readlines.map { |x| x.strip }.select { |x| x.size > 0 }
  rescue
  end

  # let ~/.flockdbs be a default set of databases
  begin
    options[:host_lists] = File.open("#{ENV['HOME']}/.flockdbs").readlines.map { |x| x.strip }.select { |x| x.size > 0 }.map { |h| h.split(",") }
  rescue
  end

  opts = OptionParser.new do |opts|
    opts.banner = "Usage: flocker.rb [options] <command>"

    opts.on("-N", "--nameserver HOSTNAME", "database host for nameserver (default: #{options[:nameserver]})") do |hostname|
      options[:nameserver] = hostname
    end
    opts.on("-E", "--environment NAME", "environment (default: #{options[:database_name]})") do |environment|
      options[:database_name] = environment
    end
    opts.on("-D", "--dev", "use localhost flapp and database, with development environment") do
      options[:flapps] = [ "localhost" ]
      options[:nameserver] = "localhost"
      options[:database_name] = "flock_development"
      options[:append_table_name] = "_copy"
    end
    opts.on("--ids", "show ids when dumping shard info") do
      options[:show_ids] = true
    end
    opts.on("--graph IDS", "limit to certain graph ids (separated by ',')") do |graphs|
      options[:graphs] = graphs.split(",").map { |g| g.to_i }
    end
    opts.on("-f", "--flapps HOSTNAMES", "flapp servers (separated by ',') (default: '#{options[:flapps].join(',')}')") do |flapps|
      options[:flapps] = flapps.split(",")
    end
    opts.on("-p", "--percent N", "migrate/copy only N percent of the eligible shards") do |percent|
      options[:percent] = percent.to_i
    end
    opts.on("-w", "--weight N", "weight of any new shards to be created") do |weight|
      options[:weight] = weight.to_i
    end
    opts.on("--force", "don't ask confirmation on copy/migrate") do
      options[:force] = true
    end

    opts.on_tail("--help", "Show this message") do
      puts opts
      puts
      usage
      exit
    end
  end

  opts.parse!(ARGV)
  options[:flapp] = options[:flapps].sort_by { rand }[0]

  if ARGV.size < 1 || ARGV[0] == "help"
    puts "Command is required."
    puts opts
    puts
    usage
    exit
  end

  options
end

options = parse_args
$options = options

$service = connect_shard_service($options[:flapp])

case ARGV[0]
when "show"
  show_forwardings
when "busy"
  show_busy
when "find"
  (usage; exit) if ARGV.size < 2 || !options[:graphs]
  find(ARGV[1].to_i)
when "dump-db"
  (usage; exit) if ARGV.size < 2
  dump_db(ARGV[1])
when "swap-db"
  (usage; exit) if ARGV.size < 3
  swap_db(ARGV[1], ARGV[2])
when "rotate-db"
  (usage; exit) if ARGV.size < 2
  rotate_db(ARGV[1])
when "weight-db"
  (usage; exit) if ARGV.size < 2
  weight_db(ARGV[1])
when "add-replica-db"
  (usage; exit) if ARGV.size < 3
  add_replica_db(ARGV[1], ARGV[2])
when "remove-replica-db"
  (usage; exit) if ARGV.size < 2
  remove_replica_db(ARGV[1])
when "set-db"
  (usage; exit) if ARGV.size < 3
  type = ARGV[2].to_sym
  (usage; exit) unless SHARD_CLASS.keys.include?(type)
  set_db(ARGV[1], type)
when "unset-db"
  (usage; exit) if ARGV.size < 3
  type = ARGV[2].to_sym
  (usage; exit) unless SHARD_CLASS.keys.include?(type)
  unset_db(ARGV[1], type)
when "set-db-for"
  (usage; exit) if ARGV.size < 3
  type = ARGV[2].to_sym
  (usage; exit) unless SHARD_CLASS.keys.include?(type)
  set_db_for(ARGV[1].to_i, type)
when "unset-db-for"
  (usage; exit) if ARGV.size < 3
  type = ARGV[2].to_sym
  (usage; exit) unless SHARD_CLASS.keys.include?(type)
  unset_db_for(ARGV[1].to_i, type)
when "migrate"
  (usage; exit) if ARGV.size < 3
  migrate(ARGV[1],ARGV[2])
when "migrate-slaves"
  (usage; exit) if ARGV.size < 3
  migrate_slaves(ARGV[1], ARGV[2])
when "migrate-one"
  (usage; exit) if ARGV.size < 3
  migrate_one(ARGV[1].to_i, ARGV[2])
when "copy-one"
  (usage; exit) if ARGV.size < 3
  copy_one(ARGV[1].to_i, ARGV[2])
when "setup-dev"
  if options[:database_name] != "flock_development"
    puts
    puts "HEY! Don't use setup-dev without -D"
    puts
    exit 1
  end
  setup_dev_shards
when "mkshards"
  (usage; exit) if ARGV.size < 2 || !options[:graphs] || (ARGV.size < 3 && !options[:host_lists])
  num_shards = ARGV[1].to_i
  host_lists = $options[:host_lists] || ARGV[2].split(";").map { |h| h.split(",") }
  if host_lists.any? { |h| h.size != host_lists[0].size }
    puts "Host list sizes are not the same: #{host_lists.inspect}"
    exit 1
  end
  make_shards(num_shards, host_lists)
when "start-all-writes"
  start_all_writes
when "stop-all-writes"
  stop_all_writes
when "start-writes"
  (usage; exit) if ARGV.size < 2
  start_writes(ARGV[1].to_i)
when "stop-writes"
  (usage; exit) if ARGV.size < 2
  stop_writes(ARGV[1].to_i)
when "check-writes"
  check_writes
when "push"
  reload_forwardings
when "flush-all"
  flush_queues
when "flush"
  (usage; exit) if ARGV.size < 2
  flush_queues_for(ARGV[1].to_i)
when "inject"
  (usage; exit) if ARGV.size < 3
  priority = ARGV[1].to_i
  job = ARGV[2]
  connect_job_service($options[:flapp]).inject_job(priority, job)
# undocumented:
when "kick-migration"
  (usage; exit) if ARGV.size < 2
  kick_migration(ARGV[1].to_i)
when "unmigrate"
  (usage; exit) if ARGV.size < 2
  unmigrate(ARGV[1].to_i)
when "rebuild-schema"
  $service.rebuild_schema()
else
  puts "I dunno that."
  puts
  usage
  exit
end
