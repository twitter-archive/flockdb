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

$options = {
  :primary_file => "databases.txt",
  :weight => 9,
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [options] <graph_id> <count>"
  opts.separator "Example: #{$0} -d databases.txt 11 500"

  opts.on("-d", "--primary=FILENAME", "read primary db hostnames from FILENAME (default: #{$options[:primary_file]})") do |filename|
    $options[:primary_file] = filename
  end
  opts.on("-s", "--secondary=FILENAME", "read secondary db hostnames from FILENAME (default: none)") do |filename|
    $options[:secondary_file] = filename
  end
  opts.on("-w", "--weight=WEIGHT", "use weight for primary (default: #{$options[:weight]})") do |weight|
    $options[:weight] = weight.to_i
  end
end

parser.parse!(ARGV)

if ARGV.size < 2
  puts
  puts parser
  puts
  exit 1
end

graph_id = ARGV[0].to_i
count = ARGV[1].to_i

primary_databases = File.open($options[:primary_file]).readlines.map { |x| x.strip }.select { |x| x.size > 0 }
secondary_databases = if $options[:secondary_file]
  File.open($options[:secondary_file]).readlines.map { |x| x.strip }.select { |x| x.size > 0 }
else
  nil
end

print "Creating bins"
STDOUT.flush
count.times do |i|
  primary_bin_hostname = primary_databases[i % primary_databases.size]
  secondary_bin_hostname = secondary_databases ? secondary_databases[i % secondary_databases.size] : nil
  lower_bound = (1 << 60) / count * i
  table_names = [ "edges_forward_#{graph_id}_#{'%03d' % i}", "edges_backward_#{graph_id}_#{'%03d' % i}" ]

  table_names.each do |table_name|
    `gizzmo create -s "INT UNSIGNED" -d "INT UNSIGNED" "#{primary_bin_hostname}" "#{table_name}" "com.twitter.flockdb.SqlShard"`
    if secondary_bin_hostname
      `gizzmo create -s "INT UNSIGNED" -d "INT UNSIGNED" "#{secondary_bin_hostname}" "#{table_name}_2" "com.twitter.flockdb.SqlShard"`
    end

    `gizzmo create "localhost" "#{table_name}_replicating" "com.twitter.gizzard.shards.ReplicatingShard"`
    `gizzmo addlink "localhost/#{table_name}_replicating" #{primary_bin_hostname}/#{table_name} #{$options[:weight]}`
    if secondary_bin_hostname
      `gizzmo addlink "localhost/#{table_name}_replicating" "#{secondary_bin_hostname}/#{table_name}_2" 1`
    end
  end

  `gizzmo addforwarding -- #{graph_id} #{lower_bound} localhost/#{table_names[0]}_replicating`
  `gizzmo addforwarding -- -#{graph_id} #{lower_bound} localhost/#{table_names[1]}_replicating`

  print "."
  print "#{i+1}" if (i + 1) % 100 == 0
  STDOUT.flush
end
puts "Done."
