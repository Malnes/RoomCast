#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

bool roomcast_http_init(void);
void roomcast_http_set_portal_enabled(bool enabled);
void roomcast_http_set_agent_enabled(bool enabled);

#ifdef __cplusplus
}
#endif
