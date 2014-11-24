-module(vbucket).

%% port api
-export([start/0, stop/0, init/0]).

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
         config_get_vbucket_by_key/1,
         get_master/1,
         get_replica/2,
         map/3,
         found_incorrect_master/2]).

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

start() ->
  PrivDir = get_priv_dir(),
  case erl_ddll:load_driver(PrivDir, ?SHARED_LIB) of
    ok ->
      ok;
    {error, already_loaded} ->
      ok;
    {error, Error} ->
      exit(erl_ddll:format_error(Error))
  end,

  spawn(?MODULE, init, []).

stop() ->
  close = call_port(close),
  ok.

-spec config_parse(ConfigData :: string()) -> ok | {error, {atom(), string()}}.
config_parse(ConfigData) ->
  call_port({?DRV_CONFIG_PARSE, ConfigData}).

-spec config_get_num_replicas() -> integer().
config_get_num_replicas() ->
  call_port({?DRV_CONFIG_GET_NUM_REPLICAS, {}}).

-spec config_get_num_vbuckets() -> integer().
config_get_num_vbuckets() ->
  call_port({?DRV_CONFIG_GET_NUM_VBUCKETS, {}}).

-spec config_get_num_servers() -> integer().
config_get_num_servers() ->
  call_port({?DRV_CONFIG_GET_NUM_SERVERS, {}}).

-spec config_get_user() -> string() | undefined.
config_get_user() ->
  call_port({?DRV_CONFIG_GET_USER, {}}).

-spec config_get_password() -> string() | undefined.
config_get_password() ->
  call_port({?DRV_CONFIG_GET_PASSWORD, {}}).

-spec config_get_server(Index :: integer()) -> {Hostname :: string(), Port :: integer()} | not_found.
config_get_server(Index) ->
  call_port({?DRV_CONFIG_GET_SERVER, Index}).

-spec config_get_couch_api_base(Index :: integer()) -> string() | undefined.
config_get_couch_api_base(_Index) ->
  not_implemented.

-spec config_get_rest_api_server(Index :: integer()) -> string() | undefined.
config_get_rest_api_server(_Index) ->
  not_implemented.

-spec config_is_config_node(Index :: integer()) -> boolean().
config_is_config_node(_Index) ->
  not_implemented.

-spec config_get_distribution_type() -> vbucket | ketama.
config_get_distribution_type() ->
  not_implemented.

-spec config_get_vbucket_by_key(Key :: string()) -> integer().
config_get_vbucket_by_key(_Key) ->
  not_implemented.

-spec get_master(Id :: integer()) -> integer().
get_master(_Id) ->
  not_implemented.

get_replica(_Id, _Replica) ->
  not_implemented.

map(_Key, _Vbucket, _Server) ->
  not_implemented.

found_incorrect_master(_Vbucket, _WrongServer) ->
  not_implemented.

init() ->
  register(?MODULE, self()),
  try open_port({spawn_driver, ?SHARED_LIB}, [binary]) of
    Port ->
      loop(Port)
    catch
      _ -> exit(failed_to_open_port)
  end.

loop(Port) ->
  receive
    {call, Caller, SendData} ->
      Port ! {self(), {command, SendData}},
      receive
        {Port, {data, RecvData}} ->
          Caller ! {?MODULE, decode(RecvData)}
      end,
      loop(Port);
    close ->
      Port ! {self(), close}
  end.

-spec call_port(close | {integer(), any()}) -> {ok, any()} | {error, {atom(), string()}}.
call_port(close) ->
  ?MODULE ! close;
call_port(Msg) ->
  ?MODULE ! {call, self(), encode(Msg)},
  receive
    {?MODULE, Result} ->
      Result
  end.

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
