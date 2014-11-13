-module(config_SUITE).

-compile(export_all).

-include("../include/vbucket.hrl").

-include_lib("common_test/include/ct.hrl").

all() -> [test_empty_config, test_basic_config].

test_empty_config(_Config) ->
  {error, _Reason} = vbucket:parse_config(""),
  {error, _Reason} = vbucket:parse_config(<<"">>).

test_basic_config(_Config) ->
  ConfigString = "{\"hashAlgorithm\": \"CRC\",\"numReplicas\": 2,\"serverList\": [\"server1:11211\",\"server2:11210\",\"server3:11211\"],\"vBucketMap\": [[ 0, 1, 2 ],[ 1, 2, 0 ],[ 2, 1, -1 ],[ 1, 2, 0 ]]}",
  {ok, Config} = vbucket:parse_config(ConfigString),

  2 = Config#vbucket_config.num_replicas.
