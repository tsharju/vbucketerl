-module(map_SUITE).

-compile(export_all).

-include("../include/vbucket.hrl").

-include_lib("common_test/include/ct.hrl").

all() -> [test_vbucket_map].

init_per_testcase(_TestCase, Config) ->
  _Pid = vbucket:start(),
  Config.

end_per_testcase(_TestCase, _Config) ->
  vbucket:stop().

test_vbucket_map(_Config) ->
  {ok, Bin} = file:read_file("../../test/config/vbucket_eight_nodes.json"),
  ConfigString = binary_to_list(Bin),
  ok = vbucket:config_parse(ConfigString),

  {VbucketId, ServerIndex} = vbucket:map("foobar"),

  6 = VbucketId,
  3 = ServerIndex,

  {"172.16.16.76", 12006} = vbucket:config_get_server(ServerIndex).
