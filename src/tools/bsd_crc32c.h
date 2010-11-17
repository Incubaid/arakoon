#ifndef __BSD_CRC32C_H__
#define __BSD_CRC32C_H__

#include <stdint.h>


#if defined(__cplusplus)
extern "C" {
#endif

uint32_t bsd_calculate_crc32c(const unsigned char *buffer, unsigned int length);
uint32_t bsd_update_crc32c(uint32_t crc32c, const unsigned char *buffer, unsigned int length);

uint32_t sse4_2_crc32c(const unsigned char *buffer,
				uint32_t length);

#if defined(__cplusplus)
}
#endif




#endif
