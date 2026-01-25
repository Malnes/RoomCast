#include "roomcast_dns.h"

#include <string.h>

#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "lwip/sockets.h"
#include "lwip/inet.h"
#include "esp_netif_types.h"

static const char *TAG = "roomcast_dns";

static TaskHandle_t g_dns_task = NULL;
static int g_dns_sock = -1;
static bool g_running = false;
static esp_ip4_addr_t g_dns_ip;

typedef struct __attribute__((packed)) {
    uint16_t id;
    uint16_t flags;
    uint16_t qdcount;
    uint16_t ancount;
    uint16_t nscount;
    uint16_t arcount;
} dns_header_t;

static size_t parse_qname(const uint8_t *buf, size_t len, size_t offset) {
    while (offset < len) {
        uint8_t label = buf[offset];
        if (label == 0) return offset + 1;
        offset += (size_t)label + 1;
    }
    return 0;
}

static void dns_task(void *arg) {
    (void)arg;
    g_dns_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (g_dns_sock < 0) {
        ESP_LOGE(TAG, "Failed to create DNS socket");
        g_running = false;
        g_dns_task = NULL;
        vTaskDelete(NULL);
        return;
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(53);

    if (bind(g_dns_sock, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        ESP_LOGE(TAG, "Failed to bind DNS socket");
        close(g_dns_sock);
        g_dns_sock = -1;
        g_running = false;
        g_dns_task = NULL;
        vTaskDelete(NULL);
        return;
    }

    uint8_t req[512];
    uint8_t resp[512];

    while (g_running) {
        struct sockaddr_in from = {0};
        socklen_t from_len = sizeof(from);
        int len = recvfrom(g_dns_sock, req, sizeof(req), 0, (struct sockaddr *)&from, &from_len);
        if (len <= 0) continue;
        if (len < (int)sizeof(dns_header_t)) continue;

        dns_header_t *hdr = (dns_header_t *)req;
        if (ntohs(hdr->qdcount) == 0) continue;

        size_t qname_end = parse_qname(req, (size_t)len, sizeof(dns_header_t));
        if (!qname_end || qname_end + 4 > (size_t)len) continue;

        size_t question_len = (qname_end - sizeof(dns_header_t)) + 4;
        if (sizeof(dns_header_t) + question_len + 16 > sizeof(resp)) continue;

        memset(resp, 0, sizeof(resp));
        dns_header_t *rh = (dns_header_t *)resp;
        rh->id = hdr->id;
        rh->flags = htons(0x8180);
        rh->qdcount = htons(1);
        rh->ancount = htons(1);

        size_t pos = sizeof(dns_header_t);
        memcpy(resp + pos, req + sizeof(dns_header_t), question_len);
        pos += question_len;

        resp[pos++] = 0xC0; // pointer to qname
        resp[pos++] = 0x0C;
        resp[pos++] = 0x00; resp[pos++] = 0x01; // A
        resp[pos++] = 0x00; resp[pos++] = 0x01; // IN
        resp[pos++] = 0x00; resp[pos++] = 0x00; resp[pos++] = 0x00; resp[pos++] = 0x3C; // TTL 60s
        resp[pos++] = 0x00; resp[pos++] = 0x04; // RDLEN

        uint32_t ip = ntohl(g_dns_ip.addr);
        resp[pos++] = (uint8_t)((ip >> 24) & 0xFF);
        resp[pos++] = (uint8_t)((ip >> 16) & 0xFF);
        resp[pos++] = (uint8_t)((ip >> 8) & 0xFF);
        resp[pos++] = (uint8_t)(ip & 0xFF);

        sendto(g_dns_sock, resp, (int)pos, 0, (struct sockaddr *)&from, from_len);
    }

    if (g_dns_sock >= 0) {
        close(g_dns_sock);
        g_dns_sock = -1;
    }
    g_dns_task = NULL;
    vTaskDelete(NULL);
}

bool roomcast_dns_start(esp_ip4_addr_t ip) {
    if (g_dns_task) return true;
    g_dns_ip = ip;
    g_running = true;
    if (xTaskCreate(dns_task, "roomcast_dns", 4096, NULL, 4, &g_dns_task) != pdPASS) {
        ESP_LOGE(TAG, "Failed to start DNS task");
        g_running = false;
        g_dns_task = NULL;
        return false;
    }
    ESP_LOGI(TAG, "DNS started");
    return true;
}

void roomcast_dns_stop(void) {
    g_running = false;
    if (g_dns_sock >= 0) {
        shutdown(g_dns_sock, SHUT_RDWR);
    }
}
