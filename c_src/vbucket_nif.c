#include "vbucket_nif.h"

static ERL_NIF_TERM config_parse_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  unsigned arg_length;
  char *conf;
  ERL_NIF_TERM result;
  VBUCKET_CONFIG_HANDLE h;

  h = vbucket_config_create();

  enif_get_list_length(env, argv[0], &arg_length);
  conf = (char *) malloc(arg_length + 1);
  enif_get_string(env, argv[0], conf, arg_length + 1, ERL_NIF_LATIN1);

  if (vbucket_config_parse(h, LIBVBUCKET_SOURCE_MEMORY, conf) == 0)
  {
    ERL_NIF_TERM config;
    ERL_NIF_TERM num_replicas;
    ERL_NIF_TERM num_vbuckets;
    ERL_NIF_TERM num_servers;

    num_replicas = enif_make_int(env, vbucket_config_get_num_replicas(h));
    num_vbuckets = enif_make_int(env, vbucket_config_get_num_vbuckets(h));
    num_servers = enif_make_int(env, vbucket_config_get_num_servers(h));

    config = enif_make_tuple3(env, num_replicas, num_vbuckets, num_servers);

    result = enif_make_tuple2(env, enif_make_atom(env, "ok"), config);
  }
  else
  {
    result = enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_string(env, vbucket_get_error_message(h), ERL_NIF_LATIN1));
  }

  vbucket_config_destroy(h);

  return result;
}

static ErlNifFunc nif_funcs[] =
  {
    {"config_parse", 1, config_parse_nif}
  };


ERL_NIF_INIT(vbucket_nif, nif_funcs, NULL, NULL, NULL, NULL)
