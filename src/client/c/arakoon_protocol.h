#ifndef __ARAKOON_PROTOCOL_H__
#define __ARAKOON_PROTOCOL_H__

#include <inttypes.h>
#include <stdlib.h>
#include "arakoon_private.h"

size_t build_req_who_master( size_t buf_size, char* buffer);

size_t build_req_set ( size_t buf_size, char* buffer,
		uint32_t key_size, const char* key,
		uint32_t value_size, const char* value) ;

size_t build_req_get ( size_t buf_size, char* buffer,
		uint32_t key_size, const char* key);

size_t build_req_delete ( size_t buf_size, char* buffer,
		uint32_t key_size, const char* key);

size_t build_req_exists( size_t buf_size, char* buffer,
		uint32_t key_size, const char* key);

size_t build_req_hello( size_t buf_size, char* buffer,
		size_t client_id_size, const char* client_id,
		size_t cluster_id_size, const char* cluster_id) ;

size_t build_req_range( size_t buf_size, char* buffer,
		size_t b_key_size, const char* b_key, int b_key_included,
		size_t e_key_size, const char* e_key, int e_key_included, int max_cnt);

ara_rc check_for_error (int* sock, ara_cluster_t* cluster, size_t err_msg_size, char* err_msg) ;



#endif // __ARAKOON_PROTOCOL_H__
