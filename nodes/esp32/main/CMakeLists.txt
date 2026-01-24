idf_component_register(
    SRCS
        "app_main.c"
        "roomcast_http.c"
        "roomcast_storage.c"
        "roomcast_wifi.c"
        "roomcast_eq.c"
        "roomcast_snapclient.c"
    INCLUDE_DIRS "."
    REQUIRES esp_http_server esp_https_ota esp_http_client nvs_flash driver
)
