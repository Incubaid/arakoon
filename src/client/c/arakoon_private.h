#ifndef __ARAKOON_PRIVATE_H__
#define __ARAKOON_PRIVATE_H__

#include "arakoon_constants.h"

struct _ara_node_identity_t {
	char name [ MXLEN_NODENAME + 1];
	char address [ MXLEN_HOSTNAME + 1];
	int port;
};
typedef struct _ara_node_identity_t ara_node_id_t;

struct _ara_node_list_elem_t {
	ara_node_id_t* cur;
	struct _ara_node_list_elem_t* next;
};
typedef struct _ara_node_list_elem_t ara_node_list_elem_t;

struct _ara_cluster_t {
	size_t cluster_id_size;
	char cluster_id [MXLEN_CLUSTERNAME + 1];
	ara_node_list_elem_t* nodes;
	char last_error[MXLEN_ERRMSG + 1];
	//char cmd_buf [MXLEN_REQ];
        char *cmd_buf;
	int master_sock;
	char master_name[MXLEN_NODENAME];
};
typedef struct _ara_cluster_t ara_cluster_t;

ara_node_id_t create_node_id (const char* name, const char* address, int port);

struct pstring {
	uint32_t size;
	char* str;
};
typedef struct pstring sized_value_t;
typedef struct pstring sized_key_t;

struct sized_kv_pair {
	uint32_t key_size;
	char* key;
	uint32_t value_size;
	char* value;
};
typedef struct sized_kv_pair sized_kv_pair_t;

struct pstring_list_elem {
	struct pstring cur;
	struct pstring_list_elem* next;
};
typedef struct pstring_list_elem* object_list_elem_t;

struct object_list_type {
	uint32_t count;
	object_list_elem_t first_ptr;
};
typedef struct object_list_type key_list_t;
typedef struct object_list_type value_list_t;
typedef struct object_list_type string_list_t;

struct sized_kv_pair_list_elem {
	struct sized_kv_pair cur;
	struct sized_kv_pair_list_elem* next;
};
typedef struct sized_kv_pair_list_elem* kv_pair_list_elem_t;

struct kv_pair_list_type {
	uint32_t count;
	kv_pair_list_elem_t first_ptr;
};
typedef struct kv_pair_list_type kv_pair_list_t;


#endif //__ARAKOON_PRIVATE_H__
