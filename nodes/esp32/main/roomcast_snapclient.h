#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

void roomcast_snapclient_start(const char *host, int port);
void roomcast_snapclient_stop(void);
bool roomcast_snapclient_is_running(void);
void roomcast_snapclient_set_volume(int percent);
void roomcast_snapclient_set_muted(bool muted);

#ifdef __cplusplus
}
#endif
