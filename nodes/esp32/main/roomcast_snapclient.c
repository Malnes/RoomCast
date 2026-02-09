#include "roomcast_snapclient.h"

#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>

#include "driver/i2s.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_mac.h"
#include "esp_heap_caps.h"
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"
#include "lwip/sockets.h"
#include "lwip/netdb.h"
#include "cJSON.h"

#include "roomcast_config.h"
#include "roomcast_storage.h"

static void load_device_name(char *out, size_t out_len) {
    if (!out || out_len == 0) return;
    if (!roomcast_storage_get_string(ROOMCAST_NVS_KEY_DEVICE_NAME, out, out_len)) {
        strncpy(out, "RoomCast ESP32", out_len - 1);
    }
}

static const char *TAG = "roomcast_snapclient";

static bool g_running = false;
static TaskHandle_t g_task = NULL;
static TaskHandle_t g_audio_task = NULL;
static QueueHandle_t g_audio_queue = NULL;
static int g_sock = -1;
static char g_host[64] = {0};
static int g_port = 1704;
static uint16_t g_msg_id = 1;

static int g_volume = 75;
static bool g_muted = false;

static int64_t g_time_diff_us = 0;
static int64_t g_last_time_sent_us = 0;
static uint16_t g_last_time_id = 0;
static int64_t g_last_time_sent_mark_us = 0;

static uint32_t g_buffer_ms = 1000;
static uint32_t g_latency_ms = 0;

static bool g_i2s_ready = false;
static int g_sample_rate = 44100;
static int g_bits_per_sample = 16;
static int g_channels = 2;

typedef struct {
    int64_t target_time_us;
    uint32_t size;
    uint8_t *data;
} roomcast_audio_chunk_t;

static uint8_t *audio_buffer_alloc(size_t size) {
    if (size == 0) return NULL;
    uint8_t *ptr = NULL;
#if defined(CONFIG_SPIRAM) && CONFIG_SPIRAM
    ptr = (uint8_t *)heap_caps_malloc(size, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
#endif
    if (!ptr) {
        ptr = (uint8_t *)heap_caps_malloc(size, MALLOC_CAP_8BIT);
    }
    return ptr;
}

typedef struct {
    uint16_t type;
    uint16_t id;
    uint16_t refers_to;
    int32_t sent_sec;
    int32_t sent_usec;
    int32_t recv_sec;
    int32_t recv_usec;
    uint32_t size;
} __attribute__((packed)) snap_base_t;

static uint16_t read_le16(const uint8_t *buf) {
    return (uint16_t)(buf[0] | (buf[1] << 8));
}

static uint32_t read_le32(const uint8_t *buf) {
    return (uint32_t)(buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24));
}

static int32_t read_le32s(const uint8_t *buf) {
    return (int32_t)read_le32(buf);
}

static void write_le16(uint8_t *buf, uint16_t value) {
    buf[0] = (uint8_t)(value & 0xFF);
    buf[1] = (uint8_t)((value >> 8) & 0xFF);
}

static void write_le32(uint8_t *buf, uint32_t value) {
    buf[0] = (uint8_t)(value & 0xFF);
    buf[1] = (uint8_t)((value >> 8) & 0xFF);
    buf[2] = (uint8_t)((value >> 16) & 0xFF);
    buf[3] = (uint8_t)((value >> 24) & 0xFF);
}

static int recv_exact(int sock, uint8_t *buffer, size_t size) {
    size_t offset = 0;
    while (offset < size) {
        int rc = recv(sock, buffer + offset, size - offset, 0);
        if (rc <= 0) return rc;
        offset += rc;
    }
    return (int)offset;
}

static bool send_message(uint16_t type, uint16_t id, uint16_t refers_to, const uint8_t *payload, uint32_t size) {
    uint8_t header[26] = {0};
    write_le16(&header[0], type);
    write_le16(&header[2], id);
    write_le16(&header[4], refers_to);

    int64_t now = esp_timer_get_time();
    int32_t sec = (int32_t)(now / 1000000);
    int32_t usec = (int32_t)(now % 1000000);
    write_le32(&header[6], (uint32_t)sec);
    write_le32(&header[10], (uint32_t)usec);
    write_le32(&header[14], 0);
    write_le32(&header[18], 0);
    write_le32(&header[22], size);

    if (send(g_sock, header, sizeof(header), 0) < 0) return false;
    if (size > 0 && payload) {
        if (send(g_sock, payload, size, 0) < 0) return false;
    }
    return true;
}

static void send_hello(void) {
    uint8_t mac[6] = {0};
    esp_read_mac(mac, ESP_MAC_WIFI_STA);
    char mac_str[18] = {0};
    snprintf(mac_str, sizeof(mac_str), "%02X:%02X:%02X:%02X:%02X:%02X",
             mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);

    char device_name[64] = {0};
    load_device_name(device_name, sizeof(device_name));
    char hostname[64] = {0};
    size_t name_len = strlen(device_name);
    size_t host_len = 0;
    for (size_t i = 0; i < name_len && host_len < sizeof(hostname) - 1; i++) {
        char c = device_name[i];
        if ((c >= 'A' && c <= 'Z')) c = (char)(c + 32);
        if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
            hostname[host_len++] = c;
        } else if (c == ' ' || c == '-' || c == '_') {
            if (host_len > 0 && hostname[host_len - 1] != '-') {
                hostname[host_len++] = '-';
            }
        }
    }
    if (host_len == 0) {
        strncpy(hostname, "roomcast-esp32", sizeof(hostname) - 1);
    }

    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "Arch", "xtensa");
    cJSON_AddStringToObject(root, "ClientName", device_name);
    cJSON_AddStringToObject(root, "HostName", hostname);
    cJSON_AddStringToObject(root, "ID", mac_str);
    cJSON_AddNumberToObject(root, "Instance", 1);
    cJSON_AddStringToObject(root, "MAC", mac_str);
    cJSON_AddStringToObject(root, "OS", "ESP-IDF");
    cJSON_AddNumberToObject(root, "SnapStreamProtocolVersion", 2);
    cJSON_AddStringToObject(root, "Version", "esp32-0.1.12");

    char *json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!json) return;

    uint32_t json_len = (uint32_t)strlen(json);
    uint8_t *payload = (uint8_t *)malloc(4 + json_len);
    if (!payload) {
        cJSON_free(json);
        return;
    }
    write_le32(payload, json_len);
    memcpy(payload + 4, json, json_len);
    send_message(5, g_msg_id++, 0, payload, 4 + json_len);
    cJSON_free(json);
    free(payload);
}

static void send_client_info(void) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "volume", g_volume);
    cJSON_AddBoolToObject(root, "muted", g_muted);
    char *json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!json) return;
    uint32_t json_len = (uint32_t)strlen(json);
    uint8_t *payload = (uint8_t *)malloc(4 + json_len);
    if (!payload) {
        cJSON_free(json);
        return;
    }
    write_le32(payload, json_len);
    memcpy(payload + 4, json, json_len);
    send_message(7, g_msg_id++, 0, payload, 4 + json_len);
    cJSON_free(json);
    free(payload);
}

static void send_time_request(void) {
    uint8_t payload[8] = {0};
    write_le32(payload, 0);
    write_le32(payload + 4, 0);
    g_last_time_id = g_msg_id;
    g_last_time_sent_us = esp_timer_get_time();
    g_last_time_sent_mark_us = g_last_time_sent_us;
    send_message(4, g_msg_id++, 0, payload, sizeof(payload));
}

static void i2s_shutdown(void) {
    if (!g_i2s_ready) return;
    i2s_zero_dma_buffer(I2S_NUM_0);
    i2s_driver_uninstall(I2S_NUM_0);
    g_i2s_ready = false;
}

static bool i2s_setup(int sample_rate, int bits_per_sample, int channels) {
    i2s_shutdown();
    i2s_config_t cfg = {
        .mode = I2S_MODE_MASTER | I2S_MODE_TX,
        .sample_rate = sample_rate,
        .bits_per_sample = bits_per_sample,
        .channel_format = (channels == 2) ? I2S_CHANNEL_FMT_RIGHT_LEFT : I2S_CHANNEL_FMT_ONLY_LEFT,
        .communication_format = I2S_COMM_FORMAT_I2S,
        .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
        .dma_buf_count = 6,
        .dma_buf_len = 256,
        .use_apll = false,
        .tx_desc_auto_clear = true,
        .fixed_mclk = 0,
    };
    i2s_pin_config_t pins = {
        .bck_io_num = ROOMCAST_I2S_BCK_GPIO,
        .ws_io_num = ROOMCAST_I2S_LRCK_GPIO,
        .data_out_num = ROOMCAST_I2S_DATA_GPIO,
        .data_in_num = I2S_PIN_NO_CHANGE,
        .mck_io_num = I2S_PIN_NO_CHANGE,
    };
    if (i2s_driver_install(I2S_NUM_0, &cfg, 0, NULL) != ESP_OK) {
        ESP_LOGE(TAG, "I2S install failed");
        return false;
    }
    if (i2s_set_pin(I2S_NUM_0, &pins) != ESP_OK) {
        ESP_LOGE(TAG, "I2S pin config failed");
        i2s_driver_uninstall(I2S_NUM_0);
        return false;
    }
    i2s_zero_dma_buffer(I2S_NUM_0);
    g_i2s_ready = true;
    return true;
}

static bool parse_wav_header(const uint8_t *payload, uint32_t size, int *sample_rate, int *bits, int *channels) {
    if (!payload || size < 44) return false;
    if (memcmp(payload, "RIFF", 4) != 0) return false;
    if (memcmp(payload + 8, "WAVE", 4) != 0) return false;
    uint32_t fmt_offset = 12;
    while (fmt_offset + 8 < size) {
        const uint8_t *chunk = payload + fmt_offset;
        uint32_t chunk_size = read_le32(chunk + 4);
        if (memcmp(chunk, "fmt ", 4) == 0) {
            if (fmt_offset + 16 >= size) return false;
            uint16_t audio_format = read_le16(chunk + 8);
            uint16_t num_channels = read_le16(chunk + 10);
            uint32_t sr = read_le32(chunk + 12);
            uint16_t bps = read_le16(chunk + 22);
            if (audio_format != 1) return false;
            *sample_rate = (int)sr;
            *bits = (int)bps;
            *channels = (int)num_channels;
            return true;
        }
        fmt_offset += 8 + chunk_size;
    }
    return false;
}

static void audio_task(void *arg) {
    (void)arg;
    while (g_running) {
        roomcast_audio_chunk_t chunk;
        if (xQueueReceive(g_audio_queue, &chunk, pdMS_TO_TICKS(200)) != pdTRUE) {
            continue;
        }
        int64_t now = esp_timer_get_time();
        if (chunk.target_time_us > now) {
            int64_t delay_us = chunk.target_time_us - now;
            if (delay_us > 0) {
                vTaskDelay(pdMS_TO_TICKS((delay_us + 999) / 1000));
            }
        }
        if (g_i2s_ready && chunk.data && chunk.size > 0) {
            size_t written = 0;
            i2s_write(I2S_NUM_0, chunk.data, chunk.size, &written, portMAX_DELAY);
        }
        free(chunk.data);
    }
    vTaskDelete(NULL);
}

static void enqueue_audio_chunk(const uint8_t *data, uint32_t size, int64_t timestamp_us) {
    if (!data || size == 0 || !g_audio_queue) return;
    roomcast_audio_chunk_t chunk = {0};
    chunk.size = size;
    chunk.target_time_us = timestamp_us;
    chunk.data = audio_buffer_alloc(size);
    if (!chunk.data) return;
    memcpy(chunk.data, data, size);
    if (xQueueSend(g_audio_queue, &chunk, 0) != pdTRUE) {
        free(chunk.data);
    }
}

static void handle_server_settings(const uint8_t *payload, uint32_t size) {
    if (size < 4) return;
    uint32_t json_size = read_le32(payload);
    if (json_size == 0 || json_size > size - 4) return;
    char *json = (char *)malloc(json_size + 1);
    if (!json) return;
    memcpy(json, payload + 4, json_size);
    json[json_size] = '\0';
    cJSON *root = cJSON_Parse(json);
    if (root) {
        cJSON *buffer_ms = cJSON_GetObjectItem(root, "bufferMs");
        cJSON *latency = cJSON_GetObjectItem(root, "latency");
        if (cJSON_IsNumber(buffer_ms)) g_buffer_ms = (uint32_t)buffer_ms->valueint;
        if (cJSON_IsNumber(latency)) g_latency_ms = (uint32_t)latency->valueint;
        cJSON_Delete(root);
    }
    free(json);
}

static void handle_time_response(const snap_base_t *base, const uint8_t *payload, uint32_t size) {
    if (!base || size < 8) return;
    int64_t now_us = esp_timer_get_time();
    if (base->refers_to != g_last_time_id) return;
    int32_t latency_sec = read_le32s(payload);
    int32_t latency_usec = read_le32s(payload + 4);
    int64_t latency_us = ((int64_t)latency_sec * 1000000LL) + latency_usec;

    int64_t t_client_sent = g_last_time_sent_mark_us;
    int64_t t_client_recv = now_us;
    int64_t t_server_sent = ((int64_t)base->sent_sec * 1000000LL) + base->sent_usec;
    int64_t t_server_recv = ((int64_t)base->recv_sec * 1000000LL) + base->recv_usec;

    int64_t latency_c2s = t_server_recv - t_client_sent + latency_us;
    int64_t latency_s2c = t_client_recv - t_server_sent + latency_us;
    g_time_diff_us = (latency_c2s - latency_s2c) / 2;
}

static void handle_codec_header(const uint8_t *payload, uint32_t size) {
    if (size < 8) return;
    uint32_t codec_len = read_le32(payload);
    if (codec_len + 8 > size) return;
    const char *codec = (const char *)(payload + 4);
    uint32_t header_size = read_le32(payload + 4 + codec_len);
    const uint8_t *header_payload = payload + 8 + codec_len;
    if (header_size > size - 8 - codec_len) return;

    if (strncmp(codec, "pcm", codec_len) == 0) {
        int sr = 44100, bits = 16, ch = 2;
        if (parse_wav_header(header_payload, header_size, &sr, &bits, &ch)) {
            g_sample_rate = sr;
            g_bits_per_sample = bits;
            g_channels = ch;
            i2s_setup(g_sample_rate, g_bits_per_sample, g_channels);
            if (!g_audio_queue) {
                g_audio_queue = xQueueCreate(8, sizeof(roomcast_audio_chunk_t));
            }
            if (!g_audio_task) {
                xTaskCreate(audio_task, "roomcast_audio", 4096, NULL, 5, &g_audio_task);
            }
        }
    } else {
        ESP_LOGW(TAG, "Unsupported codec: %.*s", (int)codec_len, codec);
    }
}

static void handle_wire_chunk(const uint8_t *payload, uint32_t size) {
    if (size < 12) return;
    int32_t sec = read_le32s(payload);
    int32_t usec = read_le32s(payload + 4);
    uint32_t data_size = read_le32(payload + 8);
    if (data_size > size - 12) return;
    int64_t timestamp_us = ((int64_t)sec * 1000000LL) + usec;
    int64_t target_server_us = timestamp_us + ((int64_t)g_buffer_ms + g_latency_ms) * 1000LL;
    int64_t target_local_us = target_server_us - g_time_diff_us;
    enqueue_audio_chunk(payload + 12, data_size, target_local_us);
}

static void snapclient_task(void *arg) {
    (void)arg;
    struct addrinfo hints = {0};
    struct addrinfo *res = NULL;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[8];
    snprintf(port_str, sizeof(port_str), "%d", g_port);

    int err = getaddrinfo(g_host, port_str, &hints, &res);
    if (err != 0 || !res) {
        ESP_LOGE(TAG, "DNS lookup failed for %s:%d", g_host, g_port);
        g_running = false;
        g_task = NULL;
        vTaskDelete(NULL);
        return;
    }

    g_sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (g_sock < 0) {
        ESP_LOGE(TAG, "Socket create failed");
        freeaddrinfo(res);
        g_running = false;
        g_task = NULL;
        vTaskDelete(NULL);
        return;
    }

    if (connect(g_sock, res->ai_addr, res->ai_addrlen) != 0) {
        ESP_LOGE(TAG, "Snapserver connect failed");
        close(g_sock);
        g_sock = -1;
        freeaddrinfo(res);
        g_running = false;
        g_task = NULL;
        vTaskDelete(NULL);
        return;
    }
    freeaddrinfo(res);

    struct timeval timeout = { .tv_sec = 0, .tv_usec = 200000 };
    setsockopt(g_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    g_running = true;
    send_hello();
    send_client_info();
    send_time_request();

    while (g_running) {
        uint8_t header_buf[26];
        int rc = recv_exact(g_sock, header_buf, sizeof(header_buf));
        if (rc == 0) {
            break;
        }
        if (rc < 0) {
            int64_t now = esp_timer_get_time();
            if (now - g_last_time_sent_us > 5000000) {
                send_time_request();
                g_last_time_sent_us = now;
            }
            continue;
        }

        snap_base_t base = {0};
        base.type = read_le16(header_buf);
        base.id = read_le16(header_buf + 2);
        base.refers_to = read_le16(header_buf + 4);
        base.sent_sec = read_le32s(header_buf + 6);
        base.sent_usec = read_le32s(header_buf + 10);
        base.recv_sec = read_le32s(header_buf + 14);
        base.recv_usec = read_le32s(header_buf + 18);
        base.size = read_le32(header_buf + 22);

        if (base.size > 0) {
            uint8_t *payload = (uint8_t *)malloc(base.size);
            if (!payload) {
                ESP_LOGE(TAG, "Out of memory for payload size %" PRIu32, base.size);
                break;
            }
            if (recv_exact(g_sock, payload, base.size) <= 0) {
                free(payload);
                break;
            }
            switch (base.type) {
                case 1:
                    handle_codec_header(payload, base.size);
                    break;
                case 2:
                    handle_wire_chunk(payload, base.size);
                    break;
                case 3:
                    handle_server_settings(payload, base.size);
                    break;
                case 4:
                    handle_time_response(&base, payload, base.size);
                    break;
                default:
                    break;
            }
            free(payload);
        }

        int64_t now = esp_timer_get_time();
        if (now - g_last_time_sent_us > 5000000) {
            send_time_request();
            g_last_time_sent_us = now;
        }
    }

    if (g_sock >= 0) {
        close(g_sock);
        g_sock = -1;
    }
    g_running = false;
    g_task = NULL;
    vTaskDelete(NULL);
}

void roomcast_snapclient_start(const char *host, int port) {
    if (!host || !host[0]) return;
    if (g_running || g_task) return;
    strncpy(g_host, host, sizeof(g_host) - 1);
    g_port = port > 0 ? port : 1704;
    xTaskCreate(snapclient_task, "roomcast_snapclient", 8192, NULL, 5, &g_task);
}

void roomcast_snapclient_stop(void) {
    g_running = false;
    if (g_sock >= 0) {
        shutdown(g_sock, SHUT_RDWR);
        close(g_sock);
        g_sock = -1;
    }
    i2s_shutdown();
}

bool roomcast_snapclient_is_running(void) {
    return g_running;
}

void roomcast_snapclient_set_volume(int percent) {
    g_volume = percent < 0 ? 0 : (percent > 100 ? 100 : percent);
    if (g_running) send_client_info();
}

void roomcast_snapclient_set_muted(bool muted) {
    g_muted = muted;
    if (g_running) send_client_info();
}
