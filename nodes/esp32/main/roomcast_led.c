#include "roomcast_led.h"
#include "roomcast_config.h"

#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "driver/rmt.h"

static const char *TAG = "roomcast_led";

static bool g_rmt_ready = false;
static rmt_channel_t g_rmt_channel = RMT_CHANNEL_0;
static TaskHandle_t g_led_task = NULL;
static roomcast_led_status_t g_status = ROOMCAST_LED_OFF;

static void set_color(uint8_t r, uint8_t g, uint8_t b) {
    if (!g_rmt_ready) return;
    rmt_item32_t items[24];
    uint32_t grb = ((uint32_t)g << 16) | ((uint32_t)r << 8) | (uint32_t)b;

    const uint32_t t0h = 16; // 0.4us @ 40MHz
    const uint32_t t0l = 34; // 0.85us
    const uint32_t t1h = 32; // 0.8us
    const uint32_t t1l = 18; // 0.45us

    for (int i = 0; i < 24; i++) {
        bool bit = (grb >> (23 - i)) & 0x1;
        if (bit) {
            items[i].level0 = 1;
            items[i].duration0 = t1h;
            items[i].level1 = 0;
            items[i].duration1 = t1l;
        } else {
            items[i].level0 = 1;
            items[i].duration0 = t0h;
            items[i].level1 = 0;
            items[i].duration1 = t0l;
        }
    }
    rmt_write_items(g_rmt_channel, items, 24, true);
    rmt_wait_tx_done(g_rmt_channel, pdMS_TO_TICKS(10));
}

static void led_task(void *arg) {
    (void)arg;
    while (true) {
        switch (g_status) {
            case ROOMCAST_LED_ERROR:
                set_color(255, 0, 0);
                vTaskDelay(pdMS_TO_TICKS(250));
                set_color(0, 0, 0);
                vTaskDelay(pdMS_TO_TICKS(250));
                break;
            case ROOMCAST_LED_PORTAL:
                set_color(0, 0, 180);
                vTaskDelay(pdMS_TO_TICKS(500));
                break;
            case ROOMCAST_LED_CONNECTED:
                set_color(0, 180, 0);
                vTaskDelay(pdMS_TO_TICKS(500));
                break;
            default:
                set_color(0, 0, 0);
                vTaskDelay(pdMS_TO_TICKS(500));
                break;
        }
    }
}

bool roomcast_led_init(void) {
    rmt_config_t config = RMT_DEFAULT_CONFIG_TX(ROOMCAST_WS2812_GPIO, g_rmt_channel);
    config.clk_div = 2; // 40MHz
    if (rmt_config(&config) != ESP_OK || rmt_driver_install(g_rmt_channel, 0, 0) != ESP_OK) {
        ESP_LOGW(TAG, "Failed to init WS2812 RMT on GPIO %d", ROOMCAST_WS2812_GPIO);
        return false;
    }
    g_rmt_ready = true;
    set_color(0, 0, 0);
    if (!g_led_task) {
        xTaskCreate(led_task, "roomcast_led", 2048, NULL, 2, &g_led_task);
    }
    return true;
}

void roomcast_led_set_status(roomcast_led_status_t status) {
    g_status = status;
}
