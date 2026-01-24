#include "roomcast_storage.h"
#include "roomcast_config.h"

#include <string.h>

#include "esp_err.h"
#include "nvs_flash.h"
#include "nvs.h"

bool roomcast_storage_init(void) {
    esp_err_t err = nvs_flash_init();
    if (err == ESP_ERR_NVS_NO_FREE_PAGES || err == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        if (nvs_flash_erase() == ESP_OK) {
            err = nvs_flash_init();
        }
    }
    return err == ESP_OK;
}

static bool open_nvs(nvs_handle_t *out) {
    if (!out) return false;
    return nvs_open(ROOMCAST_NVS_NAMESPACE, NVS_READWRITE, out) == ESP_OK;
}

bool roomcast_storage_get_string(const char *key, char *out, size_t out_len) {
    if (!key || !out || out_len == 0) return false;
    nvs_handle_t handle;
    if (!open_nvs(&handle)) return false;
    size_t size = out_len;
    esp_err_t err = nvs_get_str(handle, key, out, &size);
    nvs_close(handle);
    return err == ESP_OK;
}

bool roomcast_storage_set_string(const char *key, const char *value) {
    if (!key || !value) return false;
    nvs_handle_t handle;
    if (!open_nvs(&handle)) return false;
    esp_err_t err = nvs_set_str(handle, key, value);
    if (err == ESP_OK) err = nvs_commit(handle);
    nvs_close(handle);
    return err == ESP_OK;
}

bool roomcast_storage_get_u16(const char *key, uint16_t *out) {
    if (!key || !out) return false;
    nvs_handle_t handle;
    if (!open_nvs(&handle)) return false;
    uint16_t value = 0;
    esp_err_t err = nvs_get_u16(handle, key, &value);
    nvs_close(handle);
    if (err != ESP_OK) return false;
    *out = value;
    return true;
}

bool roomcast_storage_set_u16(const char *key, uint16_t value) {
    nvs_handle_t handle;
    if (!open_nvs(&handle)) return false;
    esp_err_t err = nvs_set_u16(handle, key, value);
    if (err == ESP_OK) err = nvs_commit(handle);
    nvs_close(handle);
    return err == ESP_OK;
}

bool roomcast_storage_get_blob(const char *key, char *out, size_t out_len) {
    if (!key || !out || out_len == 0) return false;
    nvs_handle_t handle;
    if (!open_nvs(&handle)) return false;
    size_t size = out_len;
    esp_err_t err = nvs_get_blob(handle, key, out, &size);
    nvs_close(handle);
    return err == ESP_OK;
}

bool roomcast_storage_set_blob(const char *key, const char *value) {
    if (!key || !value) return false;
    nvs_handle_t handle;
    if (!open_nvs(&handle)) return false;
    size_t len = strlen(value) + 1;
    esp_err_t err = nvs_set_blob(handle, key, value, len);
    if (err == ESP_OK) err = nvs_commit(handle);
    nvs_close(handle);
    return err == ESP_OK;
}
