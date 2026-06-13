#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>

static void serializar_be32(uint8_t *out, uint32_t v) {
    out[0] = (v >> 24) & 0xFF; out[1] = (v >> 16) & 0xFF;
    out[2] = (v >> 8) & 0xFF;  out[3] = v & 0xFF;
}

static void serializar_be16(uint8_t *out, uint16_t v) {
    out[0] = (v >> 8) & 0xFF; out[1] = v & 0xFF;
}

int main(void) {
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", "kafka:29092", errstr, sizeof(errstr));
    
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) { fprintf(stderr, "Error Kafka: %s\n", errstr); return 1; }

    printf("[C ANSI] Iniciando inyeccion de 100 sensores...\n");

    for(int i = 1; i <= 100; i++) {
        uint8_t payload[11];
        
        uint32_t ts_offset = 100000 + i;
        uint16_t sensor_num = i;
        int16_t valor_raw = 2500 + (i * 2); 
        uint16_t umbral_raw = 3000;
        
        uint8_t flags = (uint8_t)((5 << 5) | (2 << 3) | 7);

        serializar_be32(&payload[0], ts_offset);
        serializar_be16(&payload[4], sensor_num);
        serializar_be16(&payload[6], (uint16_t)valor_raw);
        serializar_be16(&payload[8], umbral_raw);
        payload[10] = flags;

        rd_kafka_producev(rk,
            RD_KAFKA_V_TOPIC("iot-embedded"),
            RD_KAFKA_V_VALUE(payload, 11),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_END);
            
        usleep(50000); 
    }

    rd_kafka_flush(rk, 10000);
    rd_kafka_destroy(rk);
    printf("[C ANSI] ¡Inyeccion completada!\n");
    return 0;
}