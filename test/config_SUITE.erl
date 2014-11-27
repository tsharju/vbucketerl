-module(config_SUITE).

-compile(export_all).

-include("../include/vbucket.hrl").

-include_lib("common_test/include/ct.hrl").

all() -> [test_empty_config, test_basic_config, test_eight_node_config].

init_per_testcase(_TestCase, Config) ->
  _Pid = vbucket:start(),
  Config.

end_per_testcase(_TestCase, _Config) ->
  vbucket:stop().

test_empty_config(_Config) ->
  {error, {parsing_failed, _}} = vbucket:config_parse("").

test_basic_config(_Config) ->
  {ok, Bin} = file:read_file("../../test/config/vbucket_basic.json"),
  ConfigString = binary_to_list(Bin),
  ok = vbucket:config_parse(ConfigString),

  2 = vbucket:config_get_num_replicas(),
  4 = vbucket:config_get_num_vbuckets(),
  3 = vbucket:config_get_num_servers(),

  undefined = vbucket:config_get_user(),
  undefined = vbucket:config_get_password(),

  {"server1", 11211} = vbucket:config_get_server(0),
  not_found = vbucket:config_get_server(3),
  not_found = vbucket:config_get_couch_api_base(3),

  undefined = vbucket:config_get_couch_api_base(0),
  undefined = vbucket:config_get_rest_api_server(0),
  vbucket = vbucket:config_get_distribution_type().

test_eight_node_config(_Config) ->
  {ok, Bin} = file:read_file("../../test/config/vbucket_eight_nodes.json"),
  ConfigString = binary_to_list(Bin),
  ok = vbucket:config_parse(ConfigString),

  2 = vbucket:config_get_num_replicas(),
  16 = vbucket:config_get_num_vbuckets(),
  8 = vbucket:config_get_num_servers(),

  {"172.16.16.76",12000} = vbucket:config_get_server(0),
  {"172.16.16.76",12006} = vbucket:config_get_server(3),

  "http://172.16.16.76:9503/default" = vbucket:config_get_couch_api_base(3),
  "http://172.16.16.76:9500/default" = vbucket:config_get_couch_api_base(0),
  "172.16.16.76:9000" = vbucket:config_get_rest_api_server(0),

  vbucket = vbucket:config_get_distribution_type().
