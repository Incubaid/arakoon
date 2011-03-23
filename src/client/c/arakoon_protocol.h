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
		uint32_t key_size, const char* key, int allow_dirty);

size_t build_req_delete ( size_t buf_size, char* buffer,
		uint32_t key_size, const char* key);

size_t build_req_exists( size_t buf_size, char* buffer,
		uint32_t key_size, const char* key, int allow_dirty);

size_t build_req_hello( size_t buf_size, char* buffer,
		size_t client_id_size, const char* client_id,
		size_t cluster_id_size, const char* cluster_id) ;

size_t build_prologue( size_t buf_size, char* buffer, ara_cluster_t* cluster);

size_t build_req_range( size_t buf_size, char* buffer,
		size_t b_key_size, const char* b_key, int b_key_included,
		size_t e_key_size, const char* e_key, int e_key_included,
		int max_cnt, int allow_dirty);

ara_rc check_for_error (int* sock, ara_cluster_t* cluster, size_t err_msg_size, char* err_msg) ;

size_t build_req_range_entries( size_t buf_size, char* buffer,
		size_t b_key_size, const char* b_key, int b_key_included,
		size_t e_key_size, const char* e_key, int e_key_included,
                int max_cnt, int allow_dirty);

size_t build_req_test_and_set(size_t buf_size, char* buffer, size_t key_size, const char* key,
                size_t old_value_size , const char* old_value, size_t new_value_size, const char* new_value);

size_t build_req_multi_get (size_t buf_size, char* buffer, key_list_t key_list, int allow_dirty);

size_t build_req_expect_progress (size_t buf_size, char *buffer);

size_t build_req_sequence (size_t buf_size, char* buffer, sequence_t kv_sequence);

size_t build_req_update(size_t buf_size, char* buffer, sequence_t kv_sequence);

int get_list_size (key_list_t key_list);

int get_pair_list_size (kv_pair_list_t key_value_list);

#endif // __ARAKOON_PROTOCOL_H__
