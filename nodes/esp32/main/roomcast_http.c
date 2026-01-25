#include "roomcast_http.h"
#include "roomcast_config.h"
#include "roomcast_storage.h"
#include "roomcast_eq.h"
#include "roomcast_snapclient.h"
#include "roomcast_wifi.h"
#include "roomcast_led.h"

#include <string.h>

#include "esp_http_server.h"
#include "esp_https_ota.h"
#include "esp_http_client.h"
#include "esp_err.h"
#include "esp_log.h"
#include "esp_mac.h"
#include "esp_system.h"
#include "esp_wifi.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "cJSON.h"

static const char *TAG = "roomcast_http";

static roomcast_eq_state_t g_eq_state;
static bool g_muted = false;
static int g_volume = 75;
static bool g_updating = false;
static int g_max_volume = 100;

static char g_agent_secret[96] = {0};
static char g_snap_host[64] = {0};
static uint16_t g_snap_port = 1704;
static char g_fingerprint[20] = {0};
static char g_ota_url[160] = {0};
static bool g_ota_task_running = false;
static httpd_handle_t g_portal_server = NULL;

static bool load_agent_secret(void) {
    return roomcast_storage_get_string(ROOMCAST_NVS_KEY_AGENT_SECRET, g_agent_secret, sizeof(g_agent_secret));
}

static bool save_agent_secret(const char *secret) {
    if (!secret) return false;
    strncpy(g_agent_secret, secret, sizeof(g_agent_secret) - 1);
    return roomcast_storage_set_string(ROOMCAST_NVS_KEY_AGENT_SECRET, g_agent_secret);
}

static bool load_snap_config(void) {
    roomcast_storage_get_string(ROOMCAST_NVS_KEY_SNAP_HOST, g_snap_host, sizeof(g_snap_host));
    roomcast_storage_get_u16(ROOMCAST_NVS_KEY_SNAP_PORT, &g_snap_port);
    return g_snap_host[0] != '\0';
}

static bool save_snap_config(const char *host, uint16_t port) {
    if (!host || !host[0]) return false;
    strncpy(g_snap_host, host, sizeof(g_snap_host) - 1);
    g_snap_port = port > 0 ? port : 1704;
    roomcast_storage_set_string(ROOMCAST_NVS_KEY_SNAP_HOST, g_snap_host);
    roomcast_storage_set_u16(ROOMCAST_NVS_KEY_SNAP_PORT, g_snap_port);
    return true;
}

static bool load_max_volume(void) {
    uint16_t value = 0;
    if (!roomcast_storage_get_u16(ROOMCAST_NVS_KEY_MAX_VOLUME, &value)) return false;
    g_max_volume = (int)value;
    if (g_max_volume < 0) g_max_volume = 0;
    if (g_max_volume > 100) g_max_volume = 100;
    return true;
}

static bool save_max_volume(int value) {
    if (value < 0) value = 0;
    if (value > 100) value = 100;
    g_max_volume = value;
    return roomcast_storage_set_u16(ROOMCAST_NVS_KEY_MAX_VOLUME, (uint16_t)value);
}

static void load_ota_url(void) {
    roomcast_storage_get_string(ROOMCAST_NVS_KEY_OTA_URL, g_ota_url, sizeof(g_ota_url));
}

static bool require_auth(httpd_req_t *req) {
    const char *header = httpd_req_get_hdr_value_len(req, "X-Agent-Secret") > 0 ? "X-Agent-Secret" : NULL;
    if (!header) {
        httpd_resp_send_err(req, HTTPD_401_UNAUTHORIZED, "Missing X-Agent-Secret");
        return false;
    }
    char secret[96] = {0};
    httpd_req_get_hdr_value_str(req, "X-Agent-Secret", secret, sizeof(secret));
    if (!g_agent_secret[0] || strcmp(secret, g_agent_secret) != 0) {
        httpd_resp_send_err(req, HTTPD_401_UNAUTHORIZED, "Invalid agent secret");
        return false;
    }
    return true;
}

static cJSON *wifi_status_json(void) {
    wifi_ap_record_t info = {0};
    if (esp_wifi_sta_get_ap_info(&info) != ESP_OK) return NULL;
    cJSON *wifi = cJSON_CreateObject();
    int rssi = info.rssi;
    int percent = (rssi + 100) * 2;
    if (percent < 0) percent = 0;
    if (percent > 100) percent = 100;
    cJSON_AddNumberToObject(wifi, "percent", percent);
    cJSON_AddNumberToObject(wifi, "signal_dbm", rssi);
    cJSON_AddStringToObject(wifi, "interface", "wifi");
    return wifi;
}

static esp_err_t health_handler(httpd_req_t *req) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "status", "ok");
    cJSON_AddBoolToObject(root, "paired", g_agent_secret[0] != '\0');
    cJSON_AddBoolToObject(root, "configured", g_snap_host[0] != '\0');
    cJSON_AddStringToObject(root, "version", "esp32-0.1.5");
    cJSON_AddBoolToObject(root, "updating", g_updating);
    cJSON_AddStringToObject(root, "playback_device", "i2s");
    cJSON *outputs = cJSON_CreateObject();
    cJSON_AddStringToObject(outputs, "selected", "i2s");
    cJSON *options = cJSON_CreateArray();
    cJSON *opt = cJSON_CreateObject();
    cJSON_AddStringToObject(opt, "id", "i2s");
    cJSON_AddStringToObject(opt, "label", "I2S DAC");
    cJSON_AddItemToArray(options, opt);
    cJSON_AddItemToObject(outputs, "options", options);
    cJSON_AddItemToObject(root, "outputs", outputs);
    cJSON_AddStringToObject(root, "fingerprint", g_fingerprint);
    cJSON_AddNumberToObject(root, "max_volume_percent", g_max_volume);
    cJSON_AddNumberToObject(root, "eq_max_bands", roomcast_eq_get_active_limit());
    cJSON_AddNumberToObject(root, "eq_active_bands", roomcast_eq_count_active(&g_eq_state));
    cJSON *wifi = wifi_status_json();
    if (wifi) {
        cJSON_AddItemToObject(root, "wifi", wifi);
    }

    cJSON *eq = cJSON_CreateObject();
    cJSON_AddStringToObject(eq, "preset", g_eq_state.preset);
    cJSON_AddNumberToObject(eq, "band_count", g_eq_state.band_count);
    cJSON *bands = cJSON_CreateArray();
    for (int i = 0; i < g_eq_state.band_count; i++) {
        cJSON *band = cJSON_CreateObject();
        cJSON_AddNumberToObject(band, "freq", g_eq_state.bands[i].freq);
        cJSON_AddNumberToObject(band, "gain", g_eq_state.bands[i].gain);
        cJSON_AddNumberToObject(band, "q", g_eq_state.bands[i].q);
        cJSON_AddItemToArray(bands, band);
    }
    cJSON_AddItemToObject(eq, "bands", bands);
    cJSON_AddItemToObject(root, "eq", eq);

    const char *json = cJSON_PrintUnformatted(root);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_sendstr(req, json);
    cJSON_free((void *)json);
    cJSON_Delete(root);
    return ESP_OK;
}

static esp_err_t pair_get_handler(httpd_req_t *req) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddBoolToObject(root, "paired", g_agent_secret[0] != '\0');
    const char *json = cJSON_PrintUnformatted(root);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_sendstr(req, json);
    cJSON_free((void *)json);
    cJSON_Delete(root);
    return ESP_OK;
}

static esp_err_t pair_post_handler(httpd_req_t *req) {
    char buf[256] = {0};
    int len = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (len < 0) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Invalid body");
        return ESP_FAIL;
    }
    cJSON *payload = cJSON_Parse(buf);
    bool force = false;
    if (payload) {
        cJSON *forceNode = cJSON_GetObjectItem(payload, "force");
        force = cJSON_IsTrue(forceNode);
    }
    if (g_agent_secret[0] && !force) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Already paired");
        cJSON_Delete(payload);
        return ESP_OK;
    }
    if (g_agent_secret[0] && force) {
        char secret[96] = {0};
        httpd_req_get_hdr_value_str(req, "X-Agent-Secret", secret, sizeof(secret));
        if (strcmp(secret, g_agent_secret) != 0) {
            httpd_resp_send_err(req, HTTPD_403_FORBIDDEN, "Node paired to another controller. Provide recovery code.");
            cJSON_Delete(payload);
            return ESP_OK;
        }
    }
    cJSON_Delete(payload);
    char new_secret[64];
    snprintf(new_secret, sizeof(new_secret), "esp32-%lu", (unsigned long)esp_random());
    save_agent_secret(new_secret);

    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "secret", g_agent_secret);
    const char *json = cJSON_PrintUnformatted(root);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_sendstr(req, json);
    cJSON_free((void *)json);
    cJSON_Delete(root);
    return ESP_OK;
}

static esp_err_t snapclient_get_handler(httpd_req_t *req) {
    cJSON *root = cJSON_CreateObject();
    cJSON *cfg = cJSON_CreateObject();
    cJSON_AddStringToObject(cfg, "snapserver_host", g_snap_host);
    cJSON_AddNumberToObject(cfg, "snapserver_port", g_snap_port);
    cJSON_AddItemToObject(root, "config", cfg);
    cJSON_AddBoolToObject(root, "configured", g_snap_host[0] != '\0');
    cJSON_AddBoolToObject(root, "running", roomcast_snapclient_is_running());
    const char *json = cJSON_PrintUnformatted(root);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_sendstr(req, json);
    cJSON_free((void *)json);
    cJSON_Delete(root);
    return ESP_OK;
}

static esp_err_t snapclient_post_handler(httpd_req_t *req) {
    if (!require_auth(req)) return ESP_FAIL;
    char buf[256] = {0};
    int len = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (len < 0) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Invalid body");
        return ESP_FAIL;
    }
    cJSON *payload = cJSON_Parse(buf);
    if (!payload) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Invalid JSON");
        return ESP_FAIL;
    }
    const cJSON *host = cJSON_GetObjectItem(payload, "snapserver_host");
    const cJSON *port = cJSON_GetObjectItem(payload, "snapserver_port");
    if (!cJSON_IsString(host) || !host->valuestring) {
        cJSON_Delete(payload);
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "snapserver_host required");
        return ESP_FAIL;
    }
    uint16_t port_val = cJSON_IsNumber(port) ? (uint16_t)port->valueint : 1704;
    save_snap_config(host->valuestring, port_val);
    roomcast_snapclient_start(g_snap_host, g_snap_port);
    cJSON_Delete(payload);
    httpd_resp_sendstr(req, "{\"ok\":true}");
    return ESP_OK;
}

static esp_err_t outputs_get_handler(httpd_req_t *req) {
    if (!require_auth(req)) return ESP_FAIL;
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "selected", "i2s");
    cJSON *options = cJSON_CreateArray();
    cJSON *opt = cJSON_CreateObject();
    cJSON_AddStringToObject(opt, "id", "i2s");
    cJSON_AddStringToObject(opt, "label", "I2S DAC");
    cJSON_AddItemToArray(options, opt);
    cJSON_AddItemToObject(root, "options", options);
    const char *json = cJSON_PrintUnformatted(root);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_sendstr(req, json);
    cJSON_free((void *)json);
    cJSON_Delete(root);
    return ESP_OK;
}

static esp_err_t outputs_post_handler(httpd_req_t *req) {
    if (!require_auth(req)) return ESP_FAIL;
    httpd_resp_sendstr(req, "{\"ok\":true,\"outputs\":{\"selected\":\"i2s\"}}");
    return ESP_OK;
}

static esp_err_t volume_post_handler(httpd_req_t *req) {
    if (!require_auth(req)) return ESP_FAIL;
    char buf[128] = {0};
    int len = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (len < 0) return ESP_FAIL;
    cJSON *payload = cJSON_Parse(buf);
    if (!payload) return ESP_FAIL;
    const cJSON *percent = cJSON_GetObjectItem(payload, "percent");
    if (cJSON_IsNumber(percent)) {
        g_volume = percent->valueint;
        if (g_volume > g_max_volume) g_volume = g_max_volume;
        if (g_volume < 0) g_volume = 0;
        roomcast_snapclient_set_volume(g_volume);
    }
    cJSON_Delete(payload);
    httpd_resp_sendstr(req, "{\"ok\":true}");
    return ESP_OK;
}

static esp_err_t max_volume_post_handler(httpd_req_t *req) {
    if (!require_auth(req)) return ESP_FAIL;
    char buf[128] = {0};
    int len = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (len < 0) return ESP_FAIL;
    cJSON *payload = cJSON_Parse(buf);
    if (!payload) return ESP_FAIL;
    const cJSON *percent = cJSON_GetObjectItem(payload, "percent");
    if (cJSON_IsNumber(percent)) {
        save_max_volume((int)percent->valueint);
    }
    cJSON_Delete(payload);
    httpd_resp_sendstr(req, "{\"ok\":true}");
    return ESP_OK;
}

static esp_err_t mute_post_handler(httpd_req_t *req) {
    if (!require_auth(req)) return ESP_FAIL;
    char buf[128] = {0};
    int len = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (len < 0) return ESP_FAIL;
    cJSON *payload = cJSON_Parse(buf);
    if (!payload) return ESP_FAIL;
    const cJSON *muted = cJSON_GetObjectItem(payload, "muted");
    if (cJSON_IsBool(muted)) {
        g_muted = cJSON_IsTrue(muted);
        roomcast_snapclient_set_muted(g_muted);
    }
    cJSON_Delete(payload);
    httpd_resp_sendstr(req, "{\"ok\":true}");
    return ESP_OK;
}

static esp_err_t eq_post_handler(httpd_req_t *req) {
    if (!require_auth(req)) return ESP_FAIL;
    char buf[1024] = {0};
    int len = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (len < 0) return ESP_FAIL;
    cJSON *payload = cJSON_Parse(buf);
    if (!payload) return ESP_FAIL;
    const cJSON *preset = cJSON_GetObjectItem(payload, "preset");
    const cJSON *bands = cJSON_GetObjectItem(payload, "bands");
    if (cJSON_IsString(preset)) {
        strncpy(g_eq_state.preset, preset->valuestring, sizeof(g_eq_state.preset) - 1);
    }
    if (cJSON_IsArray(bands)) {
        int count = cJSON_GetArraySize(bands);
        if (count > ROOMCAST_EQ_MAX_BANDS) count = ROOMCAST_EQ_MAX_BANDS;
        g_eq_state.band_count = count;
        for (int i = 0; i < count; i++) {
            cJSON *band = cJSON_GetArrayItem(bands, i);
            if (!band) continue;
            g_eq_state.bands[i].freq = cJSON_GetObjectItem(band, "freq")->valuedouble;
            g_eq_state.bands[i].gain = cJSON_GetObjectItem(band, "gain")->valuedouble;
            g_eq_state.bands[i].q = cJSON_GetObjectItem(band, "q")->valuedouble;
        }
    }
    int active = roomcast_eq_count_active(&g_eq_state);
    if (active > roomcast_eq_get_active_limit()) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "EQ band limit exceeded");
        cJSON_Delete(payload);
        return ESP_OK;
    }
    roomcast_eq_apply(&g_eq_state);
    cJSON_Delete(payload);
    char response[96];
    snprintf(response, sizeof(response), "{\"ok\":true,\"eq_active_bands\":%d,\"eq_max_bands\":%d}",
             active, roomcast_eq_get_active_limit());
    httpd_resp_sendstr(req, response);
    return ESP_OK;
}

static void ota_task(void *arg) {
    (void)arg;
    if (!g_ota_url[0]) {
        g_updating = false;
        g_ota_task_running = false;
        vTaskDelete(NULL);
        return;
    }
    ESP_LOGI(TAG, "Starting OTA from %s", g_ota_url);
    esp_http_client_config_t http_cfg = {
        .url = g_ota_url,
        .timeout_ms = 15000,
        .skip_cert_common_name_check = true,
    };
    esp_https_ota_config_t ota_cfg = {
        .http_config = &http_cfg,
    };
    esp_err_t ret = esp_https_ota(&ota_cfg);
    if (ret == ESP_OK) {
        ESP_LOGI(TAG, "OTA complete, restarting");
        esp_restart();
    } else {
        ESP_LOGE(TAG, "OTA failed: %s", esp_err_to_name(ret));
        g_updating = false;
    }
    g_ota_task_running = false;
    vTaskDelete(NULL);
}

static esp_err_t stereo_post_handler(httpd_req_t *req) {
    if (!require_auth(req)) return ESP_FAIL;
    httpd_resp_sendstr(req, "{\"ok\":true}");
    return ESP_OK;
}

static esp_err_t update_post_handler(httpd_req_t *req) {
    if (!require_auth(req)) return ESP_FAIL;
    // OTA is executed asynchronously by the main task. This handler only stores the URL.
    char buf[256] = {0};
    int len = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (len > 0) {
        cJSON *payload = cJSON_Parse(buf);
        if (payload) {
            const cJSON *url = cJSON_GetObjectItem(payload, "url");
            if (cJSON_IsString(url) && url->valuestring) {
                strncpy(g_ota_url, url->valuestring, sizeof(g_ota_url) - 1);
                roomcast_storage_set_string(ROOMCAST_NVS_KEY_OTA_URL, g_ota_url);
            }
        }
        cJSON_Delete(payload);
    }
    if (!g_ota_url[0]) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "OTA URL not set");
        return ESP_OK;
    }
    if (!g_ota_task_running) {
        g_updating = true;
        g_ota_task_running = true;
        xTaskCreate(ota_task, "roomcast_ota", 8192, NULL, 5, NULL);
    }
    httpd_resp_sendstr(req, "{\"ok\":true,\"status\":\"started\"}");
    return ESP_OK;
}

static esp_err_t restart_post_handler(httpd_req_t *req) {
    if (!require_auth(req)) return ESP_FAIL;
    httpd_resp_sendstr(req, "{\"ok\":true,\"status\":\"restarting\"}");
    esp_restart();
    return ESP_OK;
}

static esp_err_t wifi_portal_get(httpd_req_t *req) {
    const char *html =
        "<html><head><title>RoomCast Wi-Fi</title></head><body>"
        "<h2>Connect RoomCast node to Wi-Fi</h2>"
        "<form method='POST' action='/wifi'>"
        "SSID:<br/><input name='ssid' /><br/>"
        "Password:<br/><input name='pass' type='password' /><br/>"
        "<button type='submit'>Save</button>"
        "</form>"
        "</body></html>";
    httpd_resp_set_type(req, "text/html");
    httpd_resp_sendstr(req, html);
    return ESP_OK;
}

static esp_err_t captive_portal_handler(httpd_req_t *req) {
    if (strcmp(req->uri, "/") == 0 || strcmp(req->uri, "/wifi") == 0 ||
        strcmp(req->uri, "/generate_204") == 0 ||
        strcmp(req->uri, "/hotspot-detect.html") == 0 ||
        strcmp(req->uri, "/ncsi.txt") == 0) {
        return wifi_portal_get(req);
    }

    const char *location = "http://192.168.4.1/wifi";
    httpd_resp_set_status(req, "302 Found");
    httpd_resp_set_hdr(req, "Location", location);
    httpd_resp_send(req, NULL, 0);
    return ESP_OK;
}

static esp_err_t wifi_portal_post(httpd_req_t *req) {
    char buf[256] = {0};
    int len = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (len < 0) return ESP_FAIL;
    char ssid[64] = {0};
    char pass[64] = {0};
    sscanf(buf, "ssid=%63[^&]&pass=%63s", ssid, pass);
    roomcast_storage_set_string(ROOMCAST_NVS_KEY_WIFI_SSID, ssid);
    roomcast_storage_set_string(ROOMCAST_NVS_KEY_WIFI_PASS, pass);
    httpd_resp_sendstr(req, "Saved. Rebooting...");
    esp_restart();
    return ESP_OK;
}

static bool start_portal_server(void) {
    if (g_portal_server) return true;
    httpd_config_t portal_cfg = HTTPD_DEFAULT_CONFIG();
    portal_cfg.server_port = ROOMCAST_HTTP_PORT;
    portal_cfg.uri_match_fn = httpd_uri_match_wildcard;
    portal_cfg.max_uri_handlers = 8;
    portal_cfg.max_open_sockets = 4;
    portal_cfg.lru_purge_enable = true;

    if (httpd_start(&g_portal_server, &portal_cfg) != ESP_OK) {
        ESP_LOGW(TAG, "Failed to start captive portal server");
        g_portal_server = NULL;
        return false;
    }

    httpd_uri_t portal_wifi_get = {.uri = "/wifi", .method = HTTP_GET, .handler = wifi_portal_get};
    httpd_uri_t portal_wifi_post = {.uri = "/wifi", .method = HTTP_POST, .handler = wifi_portal_post};
    httpd_uri_t portal_catchall = {.uri = "/*", .method = HTTP_GET, .handler = captive_portal_handler};
    httpd_register_uri_handler(g_portal_server, &portal_wifi_get);
    httpd_register_uri_handler(g_portal_server, &portal_wifi_post);
    httpd_register_uri_handler(g_portal_server, &portal_catchall);
    ESP_LOGI(TAG, "Captive portal server started");
    return true;
}

static void stop_portal_server(void) {
    if (!g_portal_server) return;
    httpd_stop(g_portal_server);
    g_portal_server = NULL;
    ESP_LOGI(TAG, "Captive portal server stopped");
}

void roomcast_http_set_portal_enabled(bool enabled) {
    if (enabled) {
        if (!start_portal_server()) {
            roomcast_led_set_status(ROOMCAST_LED_ERROR);
        } else {
            roomcast_led_set_status(ROOMCAST_LED_PORTAL);
        }
    } else {
        stop_portal_server();
    }
}

bool roomcast_http_start(void) {
    load_agent_secret();
    roomcast_eq_init(&g_eq_state, 15);
    roomcast_eq_set_active_limit(ROOMCAST_EQ_MAX_BANDS_DEFAULT);
    load_snap_config();
    load_max_volume();
    load_ota_url();

    uint8_t mac[6] = {0};
    esp_read_mac(mac, ESP_MAC_WIFI_STA);
    snprintf(g_fingerprint, sizeof(g_fingerprint), "%02X:%02X:%02X:%02X:%02X:%02X",
             mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);

    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.server_port = ROOMCAST_AGENT_PORT;
    config.max_uri_handlers = 24;
    config.max_open_sockets = 7;
    config.lru_purge_enable = true;
    httpd_handle_t server = NULL;
    if (httpd_start(&server, &config) != ESP_OK) {
        return false;
    }

    httpd_uri_t health = {.uri = "/health", .method = HTTP_GET, .handler = health_handler};
    httpd_register_uri_handler(server, &health);

    httpd_uri_t pair_get = {.uri = "/pair", .method = HTTP_GET, .handler = pair_get_handler};
    httpd_uri_t pair_post = {.uri = "/pair", .method = HTTP_POST, .handler = pair_post_handler};
    httpd_register_uri_handler(server, &pair_get);
    httpd_register_uri_handler(server, &pair_post);

    httpd_uri_t snap_get = {.uri = "/config/snapclient", .method = HTTP_GET, .handler = snapclient_get_handler};
    httpd_uri_t snap_post = {.uri = "/config/snapclient", .method = HTTP_POST, .handler = snapclient_post_handler};
    httpd_register_uri_handler(server, &snap_get);
    httpd_register_uri_handler(server, &snap_post);

    httpd_uri_t outputs_get = {.uri = "/outputs", .method = HTTP_GET, .handler = outputs_get_handler};
    httpd_uri_t outputs_post = {.uri = "/outputs", .method = HTTP_POST, .handler = outputs_post_handler};
    httpd_register_uri_handler(server, &outputs_get);
    httpd_register_uri_handler(server, &outputs_post);

    httpd_uri_t vol_post = {.uri = "/volume", .method = HTTP_POST, .handler = volume_post_handler};
    httpd_uri_t mute_post = {.uri = "/mute", .method = HTTP_POST, .handler = mute_post_handler};
    httpd_uri_t eq_post = {.uri = "/eq", .method = HTTP_POST, .handler = eq_post_handler};
    httpd_uri_t stereo_post = {.uri = "/stereo", .method = HTTP_POST, .handler = stereo_post_handler};
    httpd_uri_t max_volume_post = {.uri = "/config/max-volume", .method = HTTP_POST, .handler = max_volume_post_handler};
    httpd_uri_t update_post = {.uri = "/update", .method = HTTP_POST, .handler = update_post_handler};
    httpd_uri_t restart_post = {.uri = "/restart", .method = HTTP_POST, .handler = restart_post_handler};

    httpd_register_uri_handler(server, &vol_post);
    httpd_register_uri_handler(server, &mute_post);
    httpd_register_uri_handler(server, &eq_post);
    httpd_register_uri_handler(server, &stereo_post);
    httpd_register_uri_handler(server, &max_volume_post);
    httpd_register_uri_handler(server, &update_post);
    httpd_register_uri_handler(server, &restart_post);

    if (!roomcast_wifi_is_connected()) {
        roomcast_http_set_portal_enabled(true);
    }

    httpd_config_t portal_config = HTTPD_DEFAULT_CONFIG();
    portal_config.server_port = ROOMCAST_HTTP_PORT;
    httpd_handle_t portal = NULL;
    if (httpd_start(&portal, &portal_config) == ESP_OK) {
        httpd_uri_t wifi_get = {.uri = "/", .method = HTTP_GET, .handler = wifi_portal_get};
        httpd_uri_t wifi_post = {.uri = "/wifi", .method = HTTP_POST, .handler = wifi_portal_post};
        httpd_register_uri_handler(portal, &wifi_get);
        httpd_register_uri_handler(portal, &wifi_post);
    }

    return true;
}
