#include "roomcast_wifi.h"
#include "roomcast_storage.h"
#include "roomcast_config.h"

#include <string.h>

#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_wifi.h"

static const char *TAG = "roomcast_wifi";
static esp_netif_t *sta_netif = NULL;
static esp_netif_t *ap_netif = NULL;
static bool wifi_ready = false;

static char g_ip[32] = "";

bool roomcast_wifi_init(void) {
    esp_err_t err = esp_netif_init();
    if (err != ESP_OK) return false;
    err = esp_event_loop_create_default();
    if (err != ESP_OK && err != ESP_ERR_INVALID_STATE) return false;
    sta_netif = esp_netif_create_default_wifi_sta();
    ap_netif = esp_netif_create_default_wifi_ap();
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    if (esp_wifi_init(&cfg) != ESP_OK) return false;
    return true;
}

bool roomcast_wifi_has_credentials(void) {
    char ssid[64] = {0};
    return roomcast_storage_get_string(ROOMCAST_NVS_KEY_WIFI_SSID, ssid, sizeof(ssid));
}

static void handle_wifi_event(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data) {
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    }
    if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        snprintf(g_ip, sizeof(g_ip), IPSTR, IP2STR(&event->ip_info.ip));
        wifi_ready = true;
    }
}

void roomcast_wifi_start(void) {
    wifi_config_t wifi_sta = {0};
    char ssid[64] = {0};
    char pass[64] = {0};
    bool has = roomcast_storage_get_string(ROOMCAST_NVS_KEY_WIFI_SSID, ssid, sizeof(ssid));
    roomcast_storage_get_string(ROOMCAST_NVS_KEY_WIFI_PASS, pass, sizeof(pass));

    esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &handle_wifi_event, NULL);
    esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &handle_wifi_event, NULL);

    if (has) {
        strncpy((char *)wifi_sta.sta.ssid, ssid, sizeof(wifi_sta.sta.ssid) - 1);
        strncpy((char *)wifi_sta.sta.password, pass, sizeof(wifi_sta.sta.password) - 1);
    }

    wifi_config_t wifi_ap = {0};
    char ap_ssid[32];
    uint8_t mac[6] = {0};
    esp_wifi_get_mac(WIFI_IF_STA, mac);
    snprintf(ap_ssid, sizeof(ap_ssid), "%s%02X%02X", ROOMCAST_SOFTAP_SSID_PREFIX, mac[4], mac[5]);
    strncpy((char *)wifi_ap.ap.ssid, ap_ssid, sizeof(wifi_ap.ap.ssid) - 1);
    wifi_ap.ap.ssid_len = strlen(ap_ssid);
    strncpy((char *)wifi_ap.ap.password, ROOMCAST_SOFTAP_PASS, sizeof(wifi_ap.ap.password) - 1);
    wifi_ap.ap.authmode = WIFI_AUTH_WPA_WPA2_PSK;
    wifi_ap.ap.channel = ROOMCAST_SOFTAP_CHANNEL;
    wifi_ap.ap.max_connection = ROOMCAST_SOFTAP_MAX_CONN;

    if (has) {
        ESP_LOGI(TAG, "Starting STA (SSID=%s) + SoftAP", ssid);
        esp_wifi_set_mode(WIFI_MODE_APSTA);
    } else {
        ESP_LOGI(TAG, "Starting SoftAP for captive setup");
        esp_wifi_set_mode(WIFI_MODE_AP);
    }

    esp_wifi_set_config(WIFI_IF_STA, &wifi_sta);
    esp_wifi_set_config(WIFI_IF_AP, &wifi_ap);
    esp_wifi_start();
}

const char *roomcast_wifi_get_ip(void) {
    return g_ip;
}
