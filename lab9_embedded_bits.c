#include <stdint.h>                              /* Declara tipos enteros de ancho fijo. */
#include <stdio.h>                               /* Declara printf para la salida de depuracion. */

#define MASK_TIPO   0xE0                         /* 1110 0000: reserva 3 bits para el tipo. */
#define MASK_ESTADO 0x18                         /* 0001 1000: reserva 2 bits para el estado. */
#define MASK_PLANTA 0x07                         /* 0000 0111: reserva 3 bits para la planta. */

typedef struct {                                 /* Estructura de trabajo en memoria local. */
    uint32_t ts_offset;                          /* Tiempo desde el encendido del equipo. */
    uint16_t sensor_num;                         /* Identificador del sensor. */
    int16_t valor_raw;                           /* Lectura cruda del sensor. */
    uint16_t umbral_raw;                         /* Umbral configurado. */
    uint8_t flags;                               /* Campo compacto de bits para tipo, estado y planta. */
    uint8_t _pad;                                /* Relleno explicito para mantener alineacion simple. */
} SensorCompacto;                                /* Nombre del tipo de estructura. */

static void set_tipo(SensorCompacto *s, uint8_t tipo) { /* Inserta el tipo en los 3 bits altos. */
    s->flags = (uint8_t)((s->flags & ~MASK_TIPO) | ((tipo << 5) & MASK_TIPO)); /* Limpia e inserta tipo sin tocar los otros bits. */
}                                                /* Fin de set_tipo. */

static void set_estado(SensorCompacto *s, uint8_t estado) { /* Inserta el estado en los bits 4 y 3. */
    s->flags = (uint8_t)((s->flags & ~MASK_ESTADO) | ((estado << 3) & MASK_ESTADO)); /* Limpia e inserta estado. */
}                                                /* Fin de set_estado. */

static void set_planta(SensorCompacto *s, uint8_t planta) { /* Inserta la planta en los 3 bits bajos. */
    s->flags = (uint8_t)((s->flags & ~MASK_PLANTA) | (planta & MASK_PLANTA)); /* Limpia e inserta planta. */
}                                                /* Fin de set_planta. */

static void imprimir_bits(uint8_t b) {           /* Imprime un byte en representacion binaria. */
    for (int i = 7; i >= 0; --i) {               /* Recorre del bit mas significativo al menos significativo. */
        printf("%d", (b >> i) & 1);              /* Extrae el bit i y lo imprime como 0 o 1. */
    }                                            /* Fin del recorrido de bits. */
}                                                /* Fin de imprimir_bits. */

static void serializar_be32(uint8_t *out, uint32_t v) { /* Escribe un uint32_t en big-endian. */
    out[0] = (uint8_t)((v >> 24) & 0xFF);        /* Byte mas significativo. */
    out[1] = (uint8_t)((v >> 16) & 0xFF);        /* Segundo byte. */
    out[2] = (uint8_t)((v >> 8) & 0xFF);         /* Tercer byte. */
    out[3] = (uint8_t)(v & 0xFF);                /* Byte menos significativo. */
}                                                /* Fin de serializar_be32. */

static void serializar_be16(uint8_t *out, uint16_t v) { /* Escribe un uint16_t en big-endian. */
    out[0] = (uint8_t)((v >> 8) & 0xFF);         /* Byte alto. */
    out[1] = (uint8_t)(v & 0xFF);                /* Byte bajo. */
}                                                /* Fin de serializar_be16. */

static void serializar_sensor(const SensorCompacto *s, uint8_t out[11]) { /* Convierte la estructura a un payload portable de 11 bytes. */
    serializar_be32(&out[0], s->ts_offset);      /* Serializa ts_offset en orden de red. */
    serializar_be16(&out[4], s->sensor_num);     /* Serializa sensor_num en orden de red. */
    serializar_be16(&out[6], (uint16_t)s->valor_raw); /* Serializa valor_raw preservando sus bits. */
    serializar_be16(&out[8], s->umbral_raw);     /* Serializa umbral_raw en orden de red. */
    out[10] = s->flags;                          /* Copia el byte de flags ya empaquetado. */
}                                                /* Fin de serializar_sensor. */

int main(void) {                                 /* Punto de entrada de la demostracion. */
    SensorCompacto s = {0};                      /* Inicializa todos los campos en cero. */
    s.ts_offset = 123456;                        /* Asigna un tiempo de ejemplo. */
    s.sensor_num = 10;                           /* Asigna un identificador de sensor. */
    s.valor_raw = -30;                           /* Asigna una lectura cruda negativa de ejemplo. */
    s.umbral_raw = 500;                          /* Asigna un umbral de ejemplo. */

    set_tipo(&s, 5);                             /* Inserta 101 en los tres bits altos. */
    set_estado(&s, 2);                           /* Inserta 10 en los bits del estado. */
    set_planta(&s, 7);                           /* Inserta 111 en los tres bits bajos. */

    printf("sizeof(SensorCompacto)=%zu\n", sizeof(SensorCompacto)); /* Muestra el tamano en memoria local. */
    printf("flags=");                            /* Etiqueta para la salida binaria. */
    imprimir_bits(s.flags);                      /* Imprime el byte de flags. */
    printf(" esperado=10110111\n");             /* Ayuda visual para comprobar el resultado. */

    uint8_t payload[11];                         /* Reserva el buffer portable para enviar por red o Kafka. */
    serializar_sensor(&s, payload);              /* Convierte la estructura al formato portable. */

    printf("payload:");                          /* Etiqueta para mostrar el payload final. */
    for (int i = 0; i < 11; ++i) {               /* Recorre cada byte del payload. */
        printf(" %02X", payload[i]);             /* Imprime cada byte en hexadecimal de dos digitos. */
    }                                            /* Fin del recorrido del payload. */
    printf("\n");                               /* Termina la linea de salida. */
    return 0;                                    /* Finaliza correctamente. */
}                                                /* Fin de main. */
