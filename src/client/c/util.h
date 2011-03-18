#ifndef __UTIL_H__
#define __UTIL_H__

#include "arakoon_private.h"

void bail(const char* msg) ;

void* checked_malloc(size_t byte_cnt);
char* bound_strncpy(char* dst, const char* src, size_t size);

int open_socket(const char* address, int port, int* sock, size_t err_msg_size, char* err_msg);

kv_pair_list_t init_kv_pair_list();
void cleanup_kv_pair_list( kv_pair_list_t* kv_pair_list);
key_list_t init_key_list();
void cleanup_key_list( key_list_t* key_list );


ara_rc recv_n_bytes(int* sock, size_t n, char* buffer, size_t timeout, size_t err_msg_size, char* err_msg);
ara_rc recv_uint32( int* sock, uint32_t* val,size_t err_msg_size, char* err_msg);
ara_rc recv_bool( int* sock, int* val,size_t err_msg_size, char* err_msg);
ara_rc recv_string( int* sock, size_t buf_size, char* buffer,size_t err_msg_size, char* err_msg);
ara_rc recv_string_option( int* sock, size_t buf_size, char* buffer, int* is_set,size_t err_msg_size, char* err_msg);
ara_rc recv_string_list(int* sock, string_list_t* list, size_t err_msg_size, char* err_msg) ;

size_t get_timeout();

ara_rc send_n_bytes(int* sock, size_t n, char* buffer, size_t timeout,size_t err_msg_size,char* err_msg);
#endif // __UTIL_H__
