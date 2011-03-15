#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>

#include "util.h"
#include "arakoon_constants.h"
#include "arakoon_private.h"

const char err_msg_create_socket[] = "Could not create socket";

key_list_t init_key_list() {
	key_list_t ret;
	ret.count = 0;
	ret.first_ptr = NULL;
	return ret;
}

void cleanup_key_list( key_list_t* kl ) {
	key_list_elem_t iter = NULL;
	key_list_elem_t prev = NULL;
	iter = kl->first_ptr;
	while (iter) {
		if( iter->cur.str ) {
			free( iter->cur.str );
		}
		prev = iter;
		iter = iter->next;
		free(prev);
	}

	kl->count = 0;
	kl->first_ptr = NULL;
}
kv_pair_list_t init_kv_pair_list () {
	kv_pair_list_t ret;
	ret.count = 0;
	ret.first_ptr = NULL;
	return ret;
}

void cleanup_kv_pair_list( kv_pair_list_t* kvp_list ) {
	kv_pair_list_elem_t iter = NULL;
	kv_pair_list_elem_t prev = NULL;
	iter = kvp_list->first_ptr;
	while( iter ) {
		if( iter->cur.key ) {
			free(iter->cur.key);
		}
		if( iter->cur.value ) {
			free(iter->cur.value );
		}
		prev = iter;
		iter = iter->next;
		free(prev);
	}
}


size_t get_timeout() {
	return 0;
}

void bail(const char* msg) {
	puts(msg);
	exit(255);
}

void* checked_malloc(size_t byte_cnt) {
	void* ret_ptr = malloc (byte_cnt);
	if (! ret_ptr ) {
		bail("Allocation failure");
	}
	return ret_ptr;
}

char* bound_strncpy(char* dst, const char* src, size_t size ) {
	if (size > 0) {
		strncpy(dst,src,size-1);
		dst[size-1]='\0';
	}
	return dst;
}


int open_socket(const char* address, int port, int* soc, size_t err_msg_size, char* err_msg) {
	int sock;
	struct hostent *host;
	struct sockaddr_in server_addr;
	host = gethostbyname(address);
	if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		bound_strncpy(err_msg, err_msg_create_socket, err_msg_size );
		return RC_CANNOT_CONNECT;
	}
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(port);
	server_addr.sin_addr = *((struct in_addr *)host->h_addr);
	bzero(&(server_addr.sin_zero),8);

	if (connect(sock, (struct sockaddr *)&server_addr,
			sizeof(struct sockaddr)) == -1)
	{
		char* msg = strerror(errno);
		bound_strncpy(err_msg,msg,err_msg_size);
		return RC_CANNOT_CONNECT;
	}
	*soc = sock;
	return 0;
}

ara_rc recv_n_bytes(int* sock, size_t n, char* buffer, size_t timeout, size_t err_msg_size, char* err_msg) {
	size_t to_read = n;
	size_t read = 0;
	int new_chunk;
	while ( to_read != 0 ) {
		new_chunk = recv(*sock,buffer+read, to_read,0);
		if ( new_chunk > 0 ) {
			to_read -= new_chunk;
			read += new_chunk;
		}
		if (new_chunk <= 0 ) {
			if ( errno == 0 ) {
				char msg[] = "Connection to remote node was closed.";
				bound_strncpy(err_msg,msg,err_msg_size);
			}
			else {
				char* msg = strerror(errno);
				bound_strncpy(err_msg,msg,err_msg_size);
			}
			close(*sock);
			*sock = -1;
			return RC_SOCKREAD_ERR;
		}
	}
	return RC_SUCCESS;
}

ara_rc recv_uint32(int* sock, uint32_t* val, size_t err_msg_size, char* err_msg) {
	return recv_n_bytes(sock, sizeof(uint32_t), (char*) val, get_timeout(), err_msg_size, err_msg );
}

ara_rc recv_string( int* sock, size_t buf_size, char* buffer, size_t err_msg_size, char* err_msg) {
	uint32_t str_size;
	int ret;
	ara_rc rc;
	RET_IF_FAILED( recv_uint32(sock, &str_size, err_msg_size, err_msg) );
	if (str_size >= buf_size ) {
		char msg[256];
		sprintf(msg,"Required buffer size=%u, actual=%zd", str_size, buf_size);
		bound_strncpy(err_msg, msg, err_msg_size);
		return RC_BUF_TOO_SMALL;
	}
	ret = recv_n_bytes(sock,str_size, buffer, get_timeout(), err_msg_size, err_msg);
	buffer[str_size] = '\0';
	return ret;
}

ara_rc send_n_bytes(int* sock, size_t n, char* buffer, size_t timeout, size_t err_msg_size, char* err_msg) {
	size_t to_send = n;
	size_t sent = 0;
	size_t sent_chunk;
	while (to_send != 0 ) {
		sent_chunk = send(*sock,buffer+sent,to_send,0);
		if (sent_chunk <= 0) {
			char* msg = strerror(errno);
			bound_strncpy(err_msg,msg,err_msg_size);
			close(*sock);
			*sock = -1;
			return RC_SOCKWRITE_ERR;
		}
		to_send -= sent_chunk;
		sent += sent_chunk;
	}
	return 0;
}

ara_rc recv_string_option( int* sock, size_t buf_size, char* buffer, int* is_set, size_t err_msg_size, char* err_msg) {
	ara_rc rc;
	RET_IF_FAILED(recv_bool(sock, is_set, err_msg_size, err_msg) );
	if ( *is_set == 1) {
		RET_IF_FAILED( recv_string(sock,buf_size,buffer, err_msg_size, err_msg));
	}
	return RC_SUCCESS;
}

ara_rc recv_bool( int* sock, int* val,size_t err_msg_size, char* err_msg) {
	char bool;
	ara_rc rc;
	RET_IF_FAILED(recv_n_bytes(sock,1,&bool,get_timeout(), err_msg_size, err_msg) );
	if (bool != '\0') {
		*val = 1;
	} else {
		*val = 0;
	}
	return RC_SUCCESS;
}
ara_rc recv_string_list(int* sock, key_list_t* key_list, size_t err_msg_size, char* err_msg) {
	ara_rc rc ;
	uint32_t i;
	*key_list = init_key_list();
	if( key_list == NULL ) {
		return RC_INVALID_POINTER;
	}
	RET_IF_FAILED( recv_uint32( sock, &(key_list->count), err_msg_size, err_msg ) );
	for ( i = 0; i < key_list->count ; i++) {
		key_list_elem_t new_elem = (key_list_elem_t) checked_malloc( sizeof( struct pstring_list_elem )) ;
		new_elem->cur.str = NULL;
		rc = recv_uint32( sock, &(new_elem->cur.size), err_msg_size, err_msg ) ;
		if( rc == RC_SUCCESS ) {
			new_elem->cur.str = (char*) checked_malloc( new_elem->cur.size + 1);
			rc = recv_n_bytes(sock, new_elem->cur.size, new_elem->cur.str, get_timeout(), err_msg_size, err_msg);
			new_elem->cur.str[new_elem->cur.size] = 0x00;
		}
		if( rc != RC_SUCCESS ) {
			cleanup_key_list( key_list );
			return rc;
		}
		new_elem->next = key_list->first_ptr;
		key_list->first_ptr = new_elem;
	}
	return rc;
}
