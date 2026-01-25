#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    ROOMCAST_LED_OFF = 0,
    ROOMCAST_LED_PORTAL,
    ROOMCAST_LED_CONNECTED,
    ROOMCAST_LED_ERROR,
} roomcast_led_status_t;

bool roomcast_led_init(void);
void roomcast_led_set_status(roomcast_led_status_t status);

#ifdef __cplusplus
}
#endif
