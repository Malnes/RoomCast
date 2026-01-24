#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ROOMCAST_EQ_MAX_BANDS 31

typedef struct {
    float freq;
    float gain;
    float q;
} roomcast_eq_band_t;

typedef struct {
    char preset[8];
    uint8_t band_count;
    roomcast_eq_band_t bands[ROOMCAST_EQ_MAX_BANDS];
} roomcast_eq_state_t;

void roomcast_eq_init(roomcast_eq_state_t *state, uint8_t band_count);
int roomcast_eq_count_active(const roomcast_eq_state_t *state);
void roomcast_eq_set_active_limit(uint8_t limit);
uint8_t roomcast_eq_get_active_limit(void);
bool roomcast_eq_apply(const roomcast_eq_state_t *state);

#ifdef __cplusplus
}
#endif
