#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "ei.h"
#include "erl_driver.h"

#include "libvbucket/vbucket.h"

#define DRV_CONFIG_PARSE 0
#define DRV_CONFIG_GET_NUM_REPLICAS 1
#define DRV_CONFIG_GET_NUM_VBUCKETS 2
#define DRV_CONFIG_GET_NUM_SERVERS 3
#define DRV_CONFIG_GET_USER 4
#define DRV_CONFIG_GET_PASSWORD 5
#define DRV_CONFIG_GET_SERVER 6
#define DRV_CONFIG_GET_COUCH_API_BASE 7
#define DRV_CONFIG_GET_REST_API_SERVER 8
#define DRV_CONFIG_IS_CONFIG_NODE 9
#define DRV_CONFIG_GET_DISTRIBUTION_TYPE 10
#define DRV_GET_MASTER 11
#define DRV_GET_REPLICA 12
#define DRV_MAP 13
#define DRV_FOUND_INCORRECT_MASTER 14

typedef struct drv_data_s {
  ErlDrvPort port;
  VBUCKET_CONFIG_HANDLE vb_config_handle;
} drv_data_t;

static void string_or_atom_undefined(const char *data, ei_x_buff *to_send)
{
  if (data != NULL)
  {
    ei_x_encode_string(to_send, data);
  }
  else
  {
    ei_x_encode_atom(to_send, "undefined");
  }
}

static void config_get_string(int command, VBUCKET_CONFIG_HANDLE h, ei_x_buff *to_send)
{
  const char *data;

  switch (command)
  {
    case DRV_CONFIG_GET_USER:
      data = vbucket_config_get_user(h);
      break;
    case DRV_CONFIG_GET_PASSWORD:
      data = vbucket_config_get_password(h);
      break;
  }

  string_or_atom_undefined(data, to_send);
}

static void get_server_host_port_tuple(const char *data, ei_x_buff *to_send, ErlDrvPort port)
{
  char* tmp;
  char* host;
  char* portnum;
  const char* search = ":";

  tmp = driver_alloc(strlen(data) + 1);
  if (tmp == NULL)
  {
    driver_failure_posix(port, errno);
  }

  strcpy(tmp, data);
  host = strsep(&tmp, search);
  portnum = tmp;

  ei_x_encode_tuple_header(to_send, 2);
  ei_x_encode_string(to_send, host);
  ei_x_encode_long(to_send, atol(portnum));
}

static void config_parse(drv_data_t *d, char *buff, ei_x_buff *to_send, int *index)
{
  char* data;
  int key_size = -1;
  int erl_type = -1;

  if (d->vb_config_handle != NULL)
  {
    // lets clear the old config
    vbucket_config_destroy(d->vb_config_handle);
  }

  d->vb_config_handle = vbucket_config_create();
  if (d->vb_config_handle == NULL)
  {
    errno = ENOMEM;
    driver_failure_posix(d->port, errno);
  }

  ei_get_type(buff, index, &erl_type, &key_size);
  data = driver_alloc(key_size);
  if (data == NULL)
  {
    driver_failure_posix(d->port, errno);
  }

  ei_decode_string(buff, index, data);

  if (vbucket_config_parse(d->vb_config_handle, LIBVBUCKET_SOURCE_MEMORY, data) == 0)
  {
    ei_x_encode_atom(to_send, "ok");
  }
  else
  {
    // failed to parse config
    ei_x_encode_tuple_header(to_send, 2);
    ei_x_encode_atom(to_send, "error");
    ei_x_encode_tuple_header(to_send, 2);
    ei_x_encode_atom(to_send, "parsing_failed");
    ei_x_encode_string(to_send, vbucket_get_error_message(d->vb_config_handle));
  }
}

static void map(ErlDrvPort port, VBUCKET_CONFIG_HANDLE h, char *buff, ei_x_buff *to_send, int *index)
{
  char *data;
  const char *server_data;
  int key_size = -1;
  int erl_type = -1;

  int vbucket_id, server_idx;

  ei_get_type(buff, index, &erl_type, &key_size);
  data = driver_alloc(key_size);
  if (data == NULL)
  {
    driver_failure_posix(port, errno);
  }
  
  ei_decode_string(buff, index, data);
  
  vbucket_map(h, data, key_size, &vbucket_id, &server_idx);
  
  ei_x_encode_tuple_header(to_send, 2);
  ei_x_encode_long(to_send, (long) vbucket_id);
  
  server_data = vbucket_config_get_server(h, server_idx);
  // split the host:port string to a string int tuple
  get_server_host_port_tuple(server_data, to_send, port);
  
  //ei_x_encode_long(to_send, (long) server_idx);
}

static void config_get_server_data(int command, ErlDrvPort port, VBUCKET_CONFIG_HANDLE h, char *buff, ei_x_buff *to_send, int *index)
{
  const char* data;

  long server_index;
  ei_decode_long(buff, index, &server_index);

  if (vbucket_config_get_num_servers(h) > server_index)
  {
    switch (command)
    {
      case DRV_CONFIG_GET_SERVER:
        data = vbucket_config_get_server(h, (int) server_index);
        // split the host:port string to a string int tuple
        get_server_host_port_tuple(data, to_send, port);
        break;
      case DRV_CONFIG_GET_COUCH_API_BASE:
        data = vbucket_config_get_couch_api_base(h, (int) server_index);
        string_or_atom_undefined(data, to_send);
        break;
      case DRV_CONFIG_GET_REST_API_SERVER:
        data = vbucket_config_get_rest_api_server(h, (int) server_index);
        string_or_atom_undefined(data, to_send);
        break;
    }
  }
  else
  {
    ei_x_encode_atom(to_send, "not_found");
  }
}

static void is_config_node(VBUCKET_CONFIG_HANDLE h, char *buff, ei_x_buff *to_send, int *index)
{
  long server_index;

  ei_decode_long(buff, index, &server_index);

  if (vbucket_config_get_num_servers(h) > server_index)
  {
    if (vbucket_config_is_config_node(h, (int) server_index) != 0)
    {
      ei_x_encode_atom(to_send, "true");
    }
    else
    {
      ei_x_encode_atom(to_send, "false");
    }
  }
  else
  {
    ei_x_encode_atom(to_send, "not_found");
  }
}

static void get_master(VBUCKET_CONFIG_HANDLE h, char *buff, ei_x_buff *to_send, int *index)
{
  long vbucket_id;

  ei_decode_long(buff, index, &vbucket_id);

  if (vbucket_id < vbucket_config_get_num_vbuckets(h))
  {
    ei_x_encode_long(to_send, (long) vbucket_get_master(h, (int) vbucket_id));
  }
  else
  {
    ei_x_encode_atom(to_send, "not_found");
  }
}

static void get_replica(VBUCKET_CONFIG_HANDLE h, char *buff, ei_x_buff *to_send, int *index)
{
  long vbucket_id;
  long replica;
  int arity;

  ei_decode_tuple_header(buff, index, &arity);
  ei_decode_long(buff, index, &vbucket_id);
  ei_decode_long(buff, index, &replica);

  if (vbucket_id < vbucket_config_get_num_vbuckets(h))
  {
    ei_x_encode_long(to_send, (long) vbucket_get_replica(h, (int) vbucket_id, (int) replica));
  }
  else
  {
    ei_x_encode_atom(to_send, "not_found");
  }
}

static void found_incorrect_master(VBUCKET_CONFIG_HANDLE h, char *buff, ei_x_buff *to_send, int *index)
{
  long vbucket_id;
  long wrong_server;
  int arity;

  ei_decode_tuple_header(buff, index, &arity);
  ei_decode_long(buff, index, &vbucket_id);
  ei_decode_long(buff, index, &wrong_server);

  if (vbucket_id < vbucket_config_get_num_vbuckets(h))
  {
    ei_x_encode_long(to_send, (long) vbucket_found_incorrect_master(h, (int) vbucket_id, (int) wrong_server));
  }
  else
  {
    ei_x_encode_atom(to_send, "not_found");
  }
}

static ErlDrvData vbucket_erl_driver_start(ErlDrvPort port, char *buffer)
{
  drv_data_t* d;

  d = (drv_data_t*)driver_alloc(sizeof(drv_data_t));
  if (d == NULL)
  {
    errno = ENOMEM;
    return ERL_DRV_ERROR_ERRNO;
  }

  d->port = port;
  d->vb_config_handle = NULL;

  return (ErlDrvData)d;
}

static void vbucket_erl_driver_stop(ErlDrvData drv_data)
{
  drv_data_t* d = (drv_data_t*)drv_data;
  if (d->vb_config_handle != NULL)
  {
    vbucket_config_destroy(d->vb_config_handle);
  }
  driver_free(drv_data);
}

static void vbucket_erl_driver_output(ErlDrvData handle, char *buff, ErlDrvSizeT bufflen)
{
  ei_x_buff to_send;

  int index = 0;
  int version;
  long command;
  int arity;

  drv_data_t* d = (drv_data_t*)handle;

  ei_decode_version(buff, &index, &version);
  ei_decode_tuple_header(buff, &index, &arity);
  ei_decode_long(buff, &index, &command);

  ei_x_new_with_version(&to_send);

  if (d->vb_config_handle == NULL && command != DRV_CONFIG_PARSE)
  {
    // config has not been parsed yet
    ei_x_encode_tuple_header(&to_send, 2);
    ei_x_encode_atom(&to_send, "error");
    ei_x_encode_atom(&to_send, "no_config");
  }
  else
  {
    switch (command)
    {
      case DRV_CONFIG_PARSE:
        config_parse(d, buff, &to_send, &index);
        break;

      case DRV_CONFIG_GET_NUM_REPLICAS:
        ei_x_encode_long(&to_send, vbucket_config_get_num_replicas(d->vb_config_handle));
        break;

      case DRV_CONFIG_GET_NUM_VBUCKETS:
        ei_x_encode_long(&to_send, vbucket_config_get_num_vbuckets(d->vb_config_handle));
        break;

      case DRV_CONFIG_GET_NUM_SERVERS:
        ei_x_encode_long(&to_send, vbucket_config_get_num_servers(d->vb_config_handle));
        break;

      case DRV_CONFIG_GET_USER:
      case DRV_CONFIG_GET_PASSWORD:
        config_get_string(command, d->vb_config_handle, &to_send);
        break;

      case DRV_CONFIG_GET_SERVER:
      case DRV_CONFIG_GET_COUCH_API_BASE:
      case DRV_CONFIG_GET_REST_API_SERVER:
        config_get_server_data(command, d->port, d->vb_config_handle, buff, &to_send, &index);
        break;

      case DRV_CONFIG_IS_CONFIG_NODE:
        is_config_node(d->vb_config_handle, buff, &to_send, &index);
        break;

      case DRV_CONFIG_GET_DISTRIBUTION_TYPE:
        if (vbucket_config_get_distribution_type(d->vb_config_handle) == VBUCKET_DISTRIBUTION_VBUCKET)
        {
          ei_x_encode_atom(&to_send, "vbucket");
        }
        else
        {
          ei_x_encode_atom(&to_send, "ketama");
        }
        break;

      case DRV_GET_MASTER:
        get_master(d->vb_config_handle, buff, &to_send, &index);
        break;

      case DRV_GET_REPLICA:
        get_replica(d->vb_config_handle, buff, &to_send, &index);
        break;

      case DRV_MAP:
        map(d->port, d->vb_config_handle, buff, &to_send, &index);
        break;

      case DRV_FOUND_INCORRECT_MASTER:
        found_incorrect_master(d->vb_config_handle, buff, &to_send, &index);
        break;
    }
  }

  driver_output(d->port, to_send.buff, to_send.index);

  ei_x_free(&to_send);
}

ErlDrvEntry vbucket_driver_entry = {
  NULL,                             /* F_PTR init, called when driver is loaded */
  vbucket_erl_driver_start,         /* L_PTR start, called when port is opened */
  vbucket_erl_driver_stop,          /* F_PTR stop, called when port is closed */
  vbucket_erl_driver_output,        /* F_PTR output, called when erlang has sent */
  NULL,                             /* F_PTR ready_input, called when input descriptor ready */
  NULL,                             /* F_PTR ready_output, called when output descriptor ready */
  "vbucket",                        /* char *driver_name, the argument to open_port */
  NULL,                             /* F_PTR finish, called when unloaded */
  NULL,                             /* void *handle, Reserved by VM */
  NULL,                             /* F_PTR control, port_command callback */
  NULL,			                        /* F_PTR timeout, reserved */
  NULL,			                        /* F_PTR outputv, reserved */
  NULL,                             /* F_PTR ready_async, only for async drivers */
  NULL,                             /* F_PTR flush, called when port is about
  		                                 to be closed, but there is data in driver queue */
  NULL,                             /* F_PTR call, much like control, sync call to driver */
  NULL,                             /* F_PTR event, called when an event selected
  		                                 by driver_event() occurs. */
  ERL_DRV_EXTENDED_MARKER,          /* int extended marker, Should always be
  		                                 set to indicate driver versioning */
  ERL_DRV_EXTENDED_MAJOR_VERSION,   /* int major_version, should always be set to this value */
  ERL_DRV_EXTENDED_MINOR_VERSION,   /* int minor_version, should always be set to this value */
  0,                                /* int driver_flags, see documentation */
  NULL,                             /* void *handle2, reserved for VM use */
  NULL,                             /* F_PTR process_exit, called when a monitored process dies */
  NULL                              /* F_PTR stop_select, called to close an event object */
};

DRIVER_INIT(vbucket)
{
  return &vbucket_driver_entry;
}
