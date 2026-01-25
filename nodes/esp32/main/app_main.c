#include "roomcast_storage.h"
#include "roomcast_wifi.h"
#include "roomcast_http.h"
#include "roomcast_snapclient.h"
#include "roomcast_led.h"
#include "roomcast_config.h"

#include "esp_log.h"

static const char *TAG = "roomcast_main";

void app_main(void) {
    if (!roomcast_storage_init()) {
        ESP_LOGE(TAG, "NVS init failed");
        return;
    }
    if (!roomcast_wifi_init()) {
        ESP_LOGE(TAG, "Wi-Fi init failed");
        return;
    }
    roomcast_led_init();
    roomcast_led_set_status(ROOMCAST_LED_PORTAL);
    if (!roomcast_http_init()) {
        ESP_LOGE(TAG, "HTTP init failed");
        return;
    }
    roomcast_wifi_start();
    roomcast_http_set_portal_enabled(true);

    // If snapserver config exists, start snapclient task.
    char host[64] = {0};
    uint16_t port = 0;
    if (roomcast_storage_get_string(ROOMCAST_NVS_KEY_SNAP_HOST, host, sizeof(host)) &&
        roomcast_storage_get_u16(ROOMCAST_NVS_KEY_SNAP_PORT, &port)) {
        roomcast_snapclient_start(host, port > 0 ? port : 1704);
    }

    ESP_LOGI(TAG, "RoomCast ESP32 node ready");
}
