-module(vbucket_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
	Procs = [{vbucket, {vbucket, start_link, []}, permanent, 5000, worker, [vbucket]}],
	{ok, {{one_for_one, 1, 5}, Procs}}.
