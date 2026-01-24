#pragma once

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

bool roomcast_storage_init(void);

bool roomcast_storage_get_string(const char *key, char *out, size_t out_len);
bool roomcast_storage_set_string(const char *key, const char *value);

bool roomcast_storage_get_u16(const char *key, uint16_t *out);
bool roomcast_storage_set_u16(const char *key, uint16_t value);

bool roomcast_storage_get_blob(const char *key, char *out, size_t out_len);
bool roomcast_storage_set_blob(const char *key, const char *value);

#ifdef __cplusplus
}
#endif
