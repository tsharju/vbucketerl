-module(vbucket_nif).

-export([config_parse/1]).

-on_load(init/0).

init() ->
    PrivDir = case code:priv_dir(?MODULE) of
        {error, bad_name} ->
	    EbinDir = filename:dirname(code:which(?MODULE)),
	    AppPath = filename:dirname(EbinDir),
	    filename:join(AppPath, "priv");
	Path ->
	    Path
	end,
    erlang:load_nif(filename:join(PrivDir, "vbucketerl"), 0).

config_parse(_) ->
    erlang:nif_error(nif_library_not_loaded).
