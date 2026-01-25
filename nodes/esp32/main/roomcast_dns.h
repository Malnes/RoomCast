#pragma once

#include <stdbool.h>
#include "esp_netif_types.h"

bool roomcast_dns_start(esp_ip4_addr_t ip);
void roomcast_dns_stop(void);
