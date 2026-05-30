#ifndef _POSIX_C_SOURCE                         /* Habilita interfaces POSIX modernas si el compilador lo requiere. */
#define _POSIX_C_SOURCE 200809L                 /* Solicita POSIX.1-2008. */
#endif                                         /* Fin de la proteccion del macro. */

#include <errno.h>                             /* Define errno y codigos de error del sistema. */
#include <stdint.h>                            /* Define enteros de ancho fijo como uint32_t. */
#include <stdio.h>                             /* Declara printf, fopen, fread y fwrite. */
#include <stdlib.h>                            /* Declara EXIT_SUCCESS y EXIT_FAILURE. */
#include <string.h>                            /* Declara memset y utilidades de memoria. */
#include <time.h>                              /* Declara time para generar timestamps de ejemplo. */

#define MAGIC_NUM   0xCAFEBABE                 /* Firma fija para reconocer nuestro archivo de buffer. */
#define MAX_RECORDS 50000                      /* Numero maximo de registros que caben en el anillo. */
#define BUFFER_FILE "/tmp/sensor_ring.bin"     /* Ruta del archivo persistente del buffer circular. */

typedef struct {                               /* Encabezado persistente del archivo. */
    uint32_t magic;                            /* Firma para validar formato correcto. */
    uint32_t max_records;                      /* Capacidad total del anillo. */
    uint32_t head;                             /* Indice donde se escribira el proximo registro. */
    uint32_t tail;                             /* Indice del registro mas antiguo disponible. */
    uint32_t count;                            /* Cantidad actual de registros validos almacenados. */
} BufferHeader;                                /* Nombre del tipo del encabezado. */

typedef struct {                               /* Registro persistente de una lectura de sensor. */
    int32_t temp_raw;                          /* Temperatura escalada por 100 para evitar flotantes. */
    uint32_t ts;                               /* Timestamp Unix de la lectura. */
    uint8_t enviado;                           /* Marca logica: 0 no enviado, 1 enviado a Kafka. */
    uint8_t reserved[3];                       /* Relleno explicito para que el layout sea estable a 12 bytes. */
} SensorData;                                  /* Nombre del tipo del dato de sensor. */

static int inicializar_buffer(void) {          /* Crea o valida el archivo persistente del anillo. */
    FILE *f = fopen(BUFFER_FILE, "rb");        /* Intenta abrir el archivo existente en modo lectura binaria. */
    if (f != NULL) {                           /* Si el archivo ya existe... */
        BufferHeader h;                        /* Reserva espacio para leer el encabezado actual. */
        size_t n = fread(&h, sizeof(h), 1, f); /* Lee exactamente un encabezado desde disco. */
        fclose(f);                             /* Cierra el archivo tras la validacion inicial. */

        if (n == 1 && h.magic == MAGIC_NUM && h.max_records == MAX_RECORDS) { /* Verifica integridad y compatibilidad basica. */
            return 0;                          /* El archivo ya es valido; no hace falta recrearlo. */
        }                                      /* Fin de la validacion del archivo existente. */
    }                                          /* Fin del caso de archivo existente. */

    f = fopen(BUFFER_FILE, "wb");             /* Crea un archivo nuevo truncando cualquier contenido invalido. */
    if (f == NULL) {                           /* Verifica si pudo crearse el archivo. */
        perror("[ERR] fopen");                 /* Informa el motivo del fallo al abrir el archivo. */
        return -1;                             /* Sale indicando error. */
    }                                          /* Fin de la validacion de creacion. */

    BufferHeader h = {MAGIC_NUM, MAX_RECORDS, 0, 0, 0}; /* Construye el encabezado inicial vacio. */
    if (fwrite(&h, sizeof(h), 1, f) != 1) {    /* Escribe el encabezado al inicio del archivo. */
        perror("[ERR] fwrite header");         /* Informa si no se pudo persistir el encabezado. */
        fclose(f);                             /* Cierra el archivo antes de salir por error. */
        return -1;                             /* Sale indicando fallo. */
    }                                          /* Fin de la escritura del encabezado. */

    SensorData empty;                          /* Reserva una estructura para rellenar el area de datos. */
    memset(&empty, 0, sizeof(empty));          /* Inicializa el registro vacio con ceros. */

    for (uint32_t i = 0; i < MAX_RECORDS; ++i) { /* Preasigna fisicamente todas las celdas del anillo. */
        if (fwrite(&empty, sizeof(empty), 1, f) != 1) { /* Escribe un registro vacio en cada posicion. */
            perror("[ERR] fwrite body");       /* Reporta fallo de escritura del cuerpo del archivo. */
            fclose(f);                         /* Cierra el archivo antes de salir. */
            return -1;                         /* Sale indicando error. */
        }                                      /* Fin de la validacion por cada celda escrita. */
    }                                          /* Fin del bucle de preasignacion. */

    fclose(f);                                 /* Cierra el archivo una vez inicializado por completo. */
    printf("[INIT] Buffer circular listo.\n"); /* Confirma que el buffer ya esta listo para usarse. */
    return 0;                                  /* Indica exito. */
}                                              /* Fin de inicializar_buffer. */

static int guardar_en_buffer(const SensorData *dato) { /* Inserta un registro dentro del anillo persistente. */
    FILE *f = fopen(BUFFER_FILE, "r+b");       /* Abre el archivo en modo lectura y escritura binaria. */
    if (f == NULL) {                           /* Verifica si se pudo abrir el archivo. */
        perror("[ERR] fopen");                 /* Reporta el error del sistema. */
        return -1;                             /* Sale con fallo. */
    }                                          /* Fin de la validacion de apertura. */

    BufferHeader h;                            /* Estructura donde leeremos el encabezado actual. */
    if (fread(&h, sizeof(h), 1, f) != 1) {     /* Lee el encabezado persistente desde el inicio del archivo. */
        perror("[ERR] fread header");          /* Reporta si no se pudo leer el encabezado. */
        fclose(f);                             /* Cierra el archivo para no dejar recursos abiertos. */
        return -1;                             /* Sale con error. */
    }                                          /* Fin de la lectura del encabezado. */

    if (h.magic != MAGIC_NUM || h.max_records != MAX_RECORDS) { /* Verifica que el archivo siga siendo compatible. */
        fprintf(stderr, "[ERR] Header invalido o corrupto.\n"); /* Informa corrupcion o formato inesperado. */
        fclose(f);                             /* Cierra el archivo antes de salir. */
        return -1;                             /* Sale con error de integridad. */
    }                                          /* Fin de la validacion de integridad. */

    long offset = (long)sizeof(BufferHeader) + (long)h.head * (long)sizeof(SensorData); /* Calcula el offset exacto de escritura. */
    if (fseek(f, offset, SEEK_SET) != 0) {     /* Mueve el puntero de archivo a la celda destino. */
        perror("[ERR] fseek data");            /* Reporta error de posicionamiento. */
        fclose(f);                             /* Cierra el archivo antes de salir. */
        return -1;                             /* Sale con error. */
    }                                          /* Fin del posicionamiento del puntero. */

    if (fwrite(dato, sizeof(*dato), 1, f) != 1) { /* Sobrescribe la celda actual con el nuevo dato. */
        perror("[ERR] fwrite data");           /* Reporta si el registro no pudo persistirse. */
        fclose(f);                             /* Cierra el archivo. */
        return -1;                             /* Sale con error. */
    }                                          /* Fin de la escritura del dato. */

    h.head = (h.head + 1) % h.max_records;     /* Avanza la cabeza del anillo de forma circular. */
    if (h.count < h.max_records) {             /* Si aun no esta lleno... */
        h.count++;                             /* Aumenta el numero de registros validos. */
    } else {                                   /* Si el buffer ya esta lleno... */
        h.tail = (h.tail + 1) % h.max_records; /* Descarta el registro mas antiguo avanzando tail. */
    }                                          /* Fin de la actualizacion del estado del anillo. */

    if (fseek(f, 0, SEEK_SET) != 0) {          /* Regresa al inicio para reescribir el encabezado. */
        perror("[ERR] fseek header");          /* Reporta error al reposicionar el archivo. */
        fclose(f);                             /* Cierra el archivo. */
        return -1;                             /* Sale con error. */
    }                                          /* Fin del reposicionamiento al encabezado. */

    if (fwrite(&h, sizeof(h), 1, f) != 1) {    /* Persiste el nuevo estado del buffer circular. */
        perror("[ERR] fwrite header");         /* Reporta fallo al escribir el encabezado actualizado. */
        fclose(f);                             /* Cierra el archivo. */
        return -1;                             /* Sale con error. */
    }                                          /* Fin de la reescritura del encabezado. */

    fclose(f);                                 /* Cierra el archivo tras completar la operacion atomica simple. */
    return 0;                                  /* Indica que el dato quedo almacenado correctamente. */
}                                              /* Fin de guardar_en_buffer. */

int main(void) {                               /* Punto de entrada del programa demostrativo. */
    if (inicializar_buffer() != 0) {           /* Garantiza que el archivo exista y sea valido. */
        return EXIT_FAILURE;                   /* Sale si la inicializacion fallo. */
    }                                          /* Fin de la validacion de inicializacion. */

    printf("[SENSOR] Insertando 5 registros...\n"); /* Mensaje de contexto para la practica. */
    for (int i = 0; i < 50000; ++i) {              /* Genera cinco muestras de ejemplo. */
        SensorData s;                          /* Reserva una estructura para la muestra actual. */
        s.temp_raw = 2500 + i;                 /* Simula temperaturas crecientes en centesimas. */
        s.ts = (uint32_t)time(NULL) + (uint32_t)i; /* Simula marcas de tiempo consecutivas. */
        s.enviado = 0;                         /* Marca inicialmente el registro como no enviado. */
        memset(s.reserved, 0, sizeof(s.reserved)); /* Limpia bytes de relleno para consistencia binaria. */

        if (guardar_en_buffer(&s) != 0) {      /* Intenta guardar la muestra en el anillo. */
            return EXIT_FAILURE;               /* Sale si ocurre un fallo de persistencia. */
        }                                      /* Fin de la insercion segura del registro. */

        printf(" -> temp_raw=%d guardado\n", s.temp_raw); /* Confirma cada insercion realizada. */
    }                                          /* Fin del bucle de prueba. */

    return EXIT_SUCCESS;                       /* Finaliza correctamente. */
}                                              /* Fin de main. */
