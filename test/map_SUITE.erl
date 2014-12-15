-module(map_SUITE).

-compile(export_all).

-include("../include/vbucket.hrl").

-include_lib("common_test/include/ct.hrl").

all() -> [test_vbucket_map].

init_per_testcase(_TestCase, Config) ->
  ok = application:start(vbucket),
  Config.

end_per_testcase(_TestCase, _Config) ->
  ok = application:stop(vbucket).

test_vbucket_map(_Config) ->
  {ok, Bin} = file:read_file("../../test/config/vbucket_eight_nodes.json"),
  ConfigString = binary_to_list(Bin),
  ok = vbucket:config_parse(ConfigString),

  {VbucketId, Server} = vbucket:map("foobar"),

  6 = VbucketId,
  {"172.16.16.76", 12006} = Server.
