#include "roomcast_eq.h"
#include "roomcast_config.h"

#include <math.h>
#include <string.h>

static uint8_t g_eq_active_limit = ROOMCAST_EQ_MAX_BANDS_DEFAULT;

void roomcast_eq_init(roomcast_eq_state_t *state, uint8_t band_count) {
    if (!state) return;
    memset(state, 0, sizeof(roomcast_eq_state_t));
    strncpy(state->preset, "peq31", sizeof(state->preset) - 1);
    state->band_count = band_count > 0 ? band_count : 15;
    if (state->band_count > ROOMCAST_EQ_MAX_BANDS) {
        state->band_count = ROOMCAST_EQ_MAX_BANDS;
    }
    for (int i = 0; i < state->band_count; i++) {
        state->bands[i].freq = 1000.0f;
        state->bands[i].gain = 0.0f;
        state->bands[i].q = 1.0f;
    }
}

int roomcast_eq_count_active(const roomcast_eq_state_t *state) {
    if (!state) return 0;
    int active = 0;
    for (int i = 0; i < state->band_count; i++) {
        if (fabsf(state->bands[i].gain) >= 0.1f) {
            active++;
        }
    }
    return active;
}

void roomcast_eq_set_active_limit(uint8_t limit) {
    if (limit < 1) limit = 1;
    if (limit > ROOMCAST_EQ_MAX_BANDS) limit = ROOMCAST_EQ_MAX_BANDS;
    g_eq_active_limit = limit;
}

uint8_t roomcast_eq_get_active_limit(void) {
    return g_eq_active_limit;
}

bool roomcast_eq_apply(const roomcast_eq_state_t *state) {
    if (!state) return false;
    // TODO: Apply EQ to DSP/I2S pipeline.
    return true;
}
