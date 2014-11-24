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

typedef struct drv_data_s {
  ErlDrvPort port;
  VBUCKET_CONFIG_HANDLE vb_config_handle;
} drv_data_t;

static void config_parse(VBUCKET_CONFIG_HANDLE h, char *buff, ei_x_buff *to_send, int *index)
{
  char* data;
  int key_size = -1;
  int erl_type = -1;

  ei_get_type(buff, index, &erl_type, &key_size);
  data = driver_alloc(key_size);
  ei_decode_string(buff, index, data);

  if (vbucket_config_parse(h, LIBVBUCKET_SOURCE_MEMORY, data) == 0)
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
    ei_x_encode_string(to_send, vbucket_get_error_message(h));
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

  if (data != NULL)
  {
    ei_x_encode_string(to_send, data);
  }
  else
  {
    ei_x_encode_atom(to_send, "undefined");
  }
}

static void config_get_server(VBUCKET_CONFIG_HANDLE h, char *buff, ei_x_buff *to_send, int *index)
{
  const char* server;

  long server_index;
  ei_decode_long(buff, index, &server_index);

  if (vbucket_config_get_num_servers(h) > server_index)
  {
    server = vbucket_config_get_server(h, (int) server_index);
    ei_x_encode_string(to_send, server);
  }
  else
  {
    ei_x_encode_atom(to_send, "not_found");
  }
}

static ErlDrvData vbucket_erl_driver_start(ErlDrvPort port, char *buffer)
{
  drv_data_t* d = (drv_data_t*)driver_alloc(sizeof(drv_data_t));
  d->port = port;
  d->vb_config_handle = vbucket_config_create();
  return (ErlDrvData)d;
}

static void vbucket_erl_driver_stop(ErlDrvData drv_data)
{
  drv_data_t* d = (drv_data_t*)drv_data;
  vbucket_config_destroy(d->vb_config_handle);
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

  switch (command)
  {
    case DRV_CONFIG_PARSE:
      config_parse(d->vb_config_handle, buff, &to_send, &index);
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
      config_get_server(d->vb_config_handle, buff, &to_send, &index);
      break;
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
