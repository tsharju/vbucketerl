-module(config_SUITE).

-compile(export_all).

-include("../include/vbucket.hrl").

-include_lib("common_test/include/ct.hrl").

all() -> [test_empty_config,
          test_basic_config,
          test_eight_node_config,
          test_config_not_parsed,
          test_config_parse_multiple_times].

init_per_testcase(_TestCase, Config) ->
  _Pid = vbucket:start(),
  Config.

end_per_testcase(_TestCase, _Config) ->
  ok.%% = vbucket:stop().

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

  true = vbucket:config_is_config_node(0),
  false = vbucket:config_is_config_node(1),
  not_found = vbucket:config_is_config_node(8),

  "http://172.16.16.76:9503/default" = vbucket:config_get_couch_api_base(3),
  "http://172.16.16.76:9500/default" = vbucket:config_get_couch_api_base(0),
  "172.16.16.76:9000" = vbucket:config_get_rest_api_server(0),

  vbucket = vbucket:config_get_distribution_type(),

  4 = vbucket:get_master(8),
  5 = vbucket:get_replica(8, 0),

  4 = vbucket:found_incorrect_master(8, 0).

test_config_not_parsed(_Config) ->
  {error, no_config} = vbucket:config_get_num_replicas(),
  {error, no_config} = vbucket:config_get_num_vbuckets(),
  {error, no_config} = vbucket:config_get_num_servers(),
  {error, no_config} = vbucket:config_get_user(),
  {error, no_config} = vbucket:config_get_password(),
  {error, no_config} = vbucket:config_get_server(0),
  {error, no_config} = vbucket:config_get_couch_api_base(0),
  {error, no_config} = vbucket:config_get_rest_api_server(0),
  {error,no_config} = vbucket:config_is_config_node(0),
  {error, no_config} = vbucket:config_get_distribution_type(),
  not_implemented = vbucket:config_get_vbucket_by_key("foobar"),
  {error, no_config} = vbucket:get_master(0),
  {error, no_config} = vbucket:get_replica(0, 0),
  {error, no_config} = vbucket:map("foobar"),
  {error, no_config} = vbucket:found_incorrect_master(0, 0).

test_config_parse_multiple_times(_Config) ->
  {ok, Bin1} = file:read_file("../../test/config/vbucket_basic.json"),
  ConfigString1 = binary_to_list(Bin1),
  ok = vbucket:config_parse(ConfigString1),

  2 = vbucket:config_get_num_replicas(),
  4 = vbucket:config_get_num_vbuckets(),
  3 = vbucket:config_get_num_servers(),

  {ok, Bin2} = file:read_file("../../test/config/vbucket_eight_nodes.json"),
  ConfigString2 = binary_to_list(Bin2),
  ok = vbucket:config_parse(ConfigString2),

  2 = vbucket:config_get_num_replicas(),
  16 = vbucket:config_get_num_vbuckets(),
  8 = vbucket:config_get_num_servers().
