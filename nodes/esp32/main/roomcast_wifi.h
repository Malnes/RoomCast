#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

bool roomcast_wifi_init(void);
bool roomcast_wifi_has_credentials(void);
void roomcast_wifi_start(void);
const char *roomcast_wifi_get_ip(void);
bool roomcast_wifi_is_connected(void);

#ifdef __cplusplus
}
#endif
