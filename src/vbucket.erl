-module(vbucket).

-behaviour(gen_server).

-export([start_link/0]).

%% libvbucket api
-export([config_parse/1,
         config_get_num_replicas/0,
         config_get_num_vbuckets/0,
         config_get_num_servers/0,
         config_get_user/0,
         config_get_password/0,
         config_get_server/1,
         config_get_couch_api_base/1,
         config_get_rest_api_server/1,
         config_is_config_node/1,
         config_get_distribution_type/0,
         get_master/1,
         get_replica/2,
         map/1,
         found_incorrect_master/2]).

%% gen_server
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-include("vbucket.hrl").

-define(SHARED_LIB, "vbucket").

-define(DRV_CONFIG_PARSE, 0).
-define(DRV_CONFIG_GET_NUM_REPLICAS, 1).
-define(DRV_CONFIG_GET_NUM_VBUCKETS, 2).
-define(DRV_CONFIG_GET_NUM_SERVERS, 3).
-define(DRV_CONFIG_GET_USER, 4).
-define(DRV_CONFIG_GET_PASSWORD, 5).
-define(DRV_CONFIG_GET_SERVER, 6).
-define(DRV_CONFIG_GET_COUCH_API_BASE, 7).
-define(DRV_CONFIG_GET_REST_API_SERVER, 8).
-define(DRV_CONFIG_IS_CONFIG_NODE, 9).
-define(DRV_CONFIG_GET_DISTRIBUTION_TYPE, 10).
-define(DRV_GET_MASTER, 11).
-define(DRV_GET_REPLICA, 12).
-define(DRV_MAP, 13).
-define(DRV_FOUND_INCORRECT_MASTER, 14).

-record(state, {port}).

start_link() ->
  PrivDir = get_priv_dir(),
  case erl_ddll:load_driver(PrivDir, ?SHARED_LIB) of
    ok ->
      ok;
    {error, already_loaded} ->
      ok;
    {error, Error} ->
      exit(erl_ddll:format_error(Error))
  end,
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec config_parse(ConfigData :: string()) -> ok | {error, {atom(), string()}}.
config_parse(ConfigData) ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_PARSE, ConfigData}}).

-spec config_get_num_replicas() -> integer() | {error, no_config}.
config_get_num_replicas() ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_GET_NUM_REPLICAS, {}}}).

-spec config_get_num_vbuckets() -> integer() | {error, no_config}.
config_get_num_vbuckets() ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_GET_NUM_VBUCKETS, {}}}).

-spec config_get_num_servers() -> integer() | {error, no_config}.
config_get_num_servers() ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_GET_NUM_SERVERS, {}}}).

-spec config_get_user() -> string() | undefined | {error, no_config}.
config_get_user() ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_GET_USER, {}}}).

-spec config_get_password() -> string() | undefined | {error, no_config}.
config_get_password() ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_GET_PASSWORD, {}}}).

-spec config_get_server(Index :: integer()) -> {Hostname :: string(), Port :: integer()} | not_found | {error, no_config}.
config_get_server(Index) ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_GET_SERVER, Index}}).

-spec config_get_couch_api_base(Index :: integer()) -> string() | undefined | {error, no_config}.
config_get_couch_api_base(Index) ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_GET_COUCH_API_BASE, Index}}).

-spec config_get_rest_api_server(Index :: integer()) -> string() | undefined | {error, no_config}.
config_get_rest_api_server(Index) ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_GET_REST_API_SERVER, Index}}).

-spec config_is_config_node(Index :: integer()) -> boolean() | not_found | {error, no_config}.
config_is_config_node(Index) ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_IS_CONFIG_NODE, Index}}).

-spec config_get_distribution_type() -> vbucket | ketama | undefined | {error, no_config}.
config_get_distribution_type() ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_CONFIG_GET_DISTRIBUTION_TYPE, {}}}).

-spec get_master(VbucketId :: integer()) -> ServerIndex :: integer() | {error, no_config}.
get_master(VbucketId) ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_GET_MASTER, VbucketId}}).

-spec get_replica(VbucketId :: integer(), Replica ::integer()) -> ServerIndex :: integer() | {error, no_config}.
get_replica(VbucketId, Replica) ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_GET_REPLICA, {VbucketId, Replica}}}).

-spec map(Key :: string()) -> {VbucketId :: integer(), {ServerHost :: string() , ServerPort :: integer()}} | {error, no_config}.
map(Key) ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_MAP, Key}}).

-spec found_incorrect_master(VbucketId :: integer(), WrongServerIndex :: integer()) -> ServerIndex :: integer() | {error, no_config}.
found_incorrect_master(VbucketId, WrongServerIndex) ->
  gen_server:call(?MODULE, {?MODULE, {?DRV_FOUND_INCORRECT_MASTER, {VbucketId, WrongServerIndex}}}).

init([]) ->
  process_flag(trap_exit, true),
  Port = open_port({spawn_driver, ?SHARED_LIB}, [binary]),
  {ok, #state{port = Port}}.

handle_call({vbucket, Msg}, _From, #state{port = Port} = State) ->
  port_command(Port, encode(Msg)),
  receive
    {Port, {data, RecvData}} ->
      {reply, decode(RecvData), State}
  end.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({'EXIT', Port, Reason}, #state{port = Port} = State) ->
  {stop, {port_terminated, Reason}, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate({port_terminated, _Reason}, _State) ->
  ok;
terminate(_Reason, #state{port = Port} = _State) ->
  port_close(Port).

encode(Msg) ->
  term_to_binary(Msg).

decode(Data) ->
  binary_to_term(<<Data/binary>>).

get_priv_dir() ->
  case code:priv_dir(?MODULE) of
    {error, bad_name} ->
      EbinDir = filename:dirname(code:which(?MODULE)),
      AppPath = filename:dirname(EbinDir),
      filename:join(AppPath, "priv");
    Path ->
      Path
  end.
