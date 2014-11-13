-module(vbucket).

-export([parse_config/1]).

-include("vbucket.hrl").

-spec parse_config(ConfigData :: iodata()) -> {ok, #vbucket_config{}} | {error, atom()}.
parse_config(ConfigData) ->
  try jiffy:decode(ConfigData) of
    C ->
      build_config(C)
  catch
    {error, Reason} ->
      {error, Reason}
  end.

%% internal

build_config({ConfigObj}) ->
  build_config(ConfigObj, #vbucket_config{});
build_config(_) ->
  {error, not_valid}.

build_config([], Config) ->
  {ok, Config};
build_config([{<<"numReplicas">>, N} | Rest], Config) ->
  build_config(Rest, Config#vbucket_config{num_replicas=N});
build_config([{<<"vBucketServerMap">>, M} | Rest] Config) ->
  {Vbuckets, NumReplicas, HashAlgorithm, Servers} = handle_server_map(M);
  build_config(Rest, Config#vbucket_config{vbuckets=VBuckets});
build_config([{_, _} | Rest], Config) ->
  build_config(Rest, Config).

handle_server_map(ServerMap) ->
  handle_server_map(ServerMap, [], 0, crc, []).

handle_server_map([], Vbuckets, NumReplicas, HashAlgorithm, Servers) ->
  {Vbuckets, NumReplicas, HashAlgorithm, Servers};
handle_server_map([{<<"vBucketMap">>, Vbuckets} | Rest], _Vbuckets, NumReplicas, HashAlgorithm, Servers) ->
  handle_server_map(Rest, Vbuckets)
