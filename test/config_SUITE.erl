-module(config_SUITE).

-compile(export_all).

-include("../include/vbucket.hrl").

-include_lib("common_test/include/ct.hrl").

all() -> [test_empty_config, test_basic_config, test_server_index_out_of_bounds].

init_per_testcase(_TestCase, Config) ->
  _Pid = vbucket:start(),
  Config.

end_per_testcase(_TestCase, _Config) ->
  vbucket:stop().

test_empty_config(_Config) ->
  {error, {parsing_failed, _}} = vbucket:config_parse("").

test_basic_config(_Config) ->
  ConfigString = "{\"hashAlgorithm\": \"CRC\",\"numReplicas\": 2,\"serverList\": [\"server1:11211\",\"server2:11210\",\"server3:11211\"],\"vBucketMap\": [[ 0, 1, 2 ],[ 1, 2, 0 ],[ 2, 1, -1 ],[ 1, 2, 0 ]]}",
  ok = vbucket:config_parse(ConfigString),

  2 = vbucket:config_get_num_replicas(),
  4 = vbucket:config_get_num_vbuckets(),
  3 = vbucket:config_get_num_servers(),

  undefined = vbucket:config_get_user(),
  undefined = vbucket:config_get_password(),

  {"server1", 11211} = vbucket:config_get_server(0),

  undefined = vbucket:config_get_couch_api_base(0).

test_server_index_out_of_bounds(_Config) ->
  ConfigString = "{\"hashAlgorithm\": \"CRC\",\"numReplicas\": 2,\"serverList\": [\"server1:11211\",\"server2:11210\",\"server3:11211\"],\"vBucketMap\": [[ 0, 1, 2 ],[ 1, 2, 0 ],[ 2, 1, -1 ],[ 1, 2, 0 ]]}",
  ok = vbucket:config_parse(ConfigString),

  not_found = vbucket:config_get_server(3),
  not_found = vbucket:config_get_server(10),

  not_found = vbucket:config_get_couch_api_base(3),
  not_found = vbucket:config_get_couch_api_base(10).
