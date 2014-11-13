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
build_config([{_, _} | Rest], Config) ->
  build_config(Rest, Config).
