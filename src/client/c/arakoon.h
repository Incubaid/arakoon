#ifndef __ARAKOON_CLIENT_H__
#define __ARAKOON_CLIENT_H__

#include "arakoon_private.h"
#include "arakoon_protocol.h"


ara_cluster_t create_empty_cluster(size_t name_size, const char* name);
ara_cluster_t add_node_to_cluster (ara_cluster_t cluster, const char* node_name, const char* address, int port);
void cleanup_cluster(ara_cluster_t* cluster);
ara_cluster_t load_cluster( const char* file_path );

int get_last_error (ara_cluster_t* cluster, size_t buf_size, char* buf);

ara_rc who_master(ara_cluster_t* cluster, size_t max_master_size, char* master);
ara_rc hello(ara_cluster_t* cluster, size_t client_id_size, const char* client_id,size_t max_response_size, char* response);
ara_rc set(ara_cluster_t* cluster, size_t key_size, const char* key, size_t value_size, const char* value);
ara_rc get(ara_cluster_t* cluster, size_t key_size, const char* key, int allow_dirty, size_t max_val_size, char* value);
ara_rc delete(ara_cluster_t* cluster, size_t key_size, const char* key );
ara_rc exists(ara_cluster_t* cluster, size_t key_size, const char* key, int allow_dirty, int* key_exists );
ara_rc range(ara_cluster_t* cluster, size_t b_key_size, const char* b_key, int b_key_included,
		size_t e_key_size, const char* e_key, int e_key_included, int max_cnt, int allow_dirty, key_list_t* key_list);
#endif //__ARAKOON_CLIENT_H__
