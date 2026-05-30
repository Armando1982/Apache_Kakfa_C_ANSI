#ifndef _POSIX_C_SOURCE                         /* Asegura que el compilador exponga interfaces POSIX necesarias. */
#define _POSIX_C_SOURCE 200809L                 /* Solicita POSIX.1-2008 para funciones como sigaction y mmap. */
#endif                                         /* Fin de la proteccion del macro de conformidad. */

#include <errno.h>                             /* Define codigos de error como EAGAIN y errno. */
#include <fcntl.h>                             /* Declara open y banderas como O_RDONLY. */
#include <signal.h>                            /* Declara manejo de senales como SIGINT. */
#include <stdio.h>                             /* Declara printf, fprintf y perror. */
#include <stdlib.h>                            /* Declara EXIT_SUCCESS y utilidades generales. */
#include <string.h>                            /* Declara memchr y strlen. */
#include <sys/mman.h>                          /* Declara mmap y munmap para mapear archivos en memoria. */
#include <sys/stat.h>                          /* Declara struct stat y fstat. */
#include <unistd.h>                            /* Declara close y otras funciones POSIX. */
#include <librdkafka/rdkafka.h>                /* API nativa del cliente Kafka en C. */

static volatile sig_atomic_t g_running = 1;    /* Bandera global segura para salir del bucle al recibir Ctrl+C. */

static void on_stop(int signo) {               /* Manejador de senal para detener el proceso con elegancia. */
    (void)signo;                               /* Evita advertencias si no usamos el parametro. */
    g_running = 0;                             /* Ordena al bucle principal finalizar cuanto antes. */
}                                              /* Fin del manejador de senal. */

static int enviar_linea(rd_kafka_t *rk,        /* Funcion auxiliar que publica una sola linea en Kafka. */
                        const char *topic,     /* Nombre del topico de destino. */
                        const char *data,      /* Puntero al inicio de la linea dentro del mmap. */
                        size_t len) {          /* Longitud exacta de la linea en bytes. */
    rd_kafka_resp_err_t err;                   /* Variable para capturar el resultado de librdkafka. */

    do {                                       /* Reintenta si la cola local del productor esta llena. */
        err = rd_kafka_producev(               /* API moderna y flexible de produccion en librdkafka. */
            rk,                                /* Instancia del productor ya configurada. */
            RD_KAFKA_V_TOPIC(topic),           /* Indica el topico donde se publicara el mensaje. */
            RD_KAFKA_V_VALUE((void *)data, len), /* Entrega el payload con puntero y longitud. */
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), /* Pide a librdkafka copiar el mensaje para seguridad. */
            RD_KAFKA_V_END);                   /* Marca el final de la lista variable de parametros. */

        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) { /* Detecta saturacion temporal de la cola local. */
            rd_kafka_poll(rk, 100);            /* Da tiempo a librdkafka para entregar mensajes pendientes. */
        }                                      /* Fin del manejo de cola llena. */
    } while (err == RD_KAFKA_RESP_ERR__QUEUE_FULL && g_running); /* Reintenta mientras siga saturada y el usuario no cancele. */

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {   /* Valida si la publicacion finalmente fallo. */
        fprintf(stderr, "[ERR] producev: %s\n", rd_kafka_err2str(err)); /* Informa la causa concreta del error. */
        return -1;                             /* Devuelve error al llamador. */
    }                                          /* Fin de la validacion de envio. */

    return 0;                                  /* Indica que el mensaje quedo aceptado por la cola del productor. */
}                                              /* Fin de la funcion enviar_linea. */

static int procesar_bulk_mmap(const char *filepath, /* Procesa un CSV grande usando lectura mapeada en memoria. */
                              rd_kafka_t *rk) {     /* Recibe la instancia del productor Kafka. */
    int fd = open(filepath, O_RDONLY);         /* Abre el archivo en modo solo lectura. */
    if (fd == -1) {                            /* Verifica si open fallo. */
        perror("[ERR] open");                  /* Imprime el error del sistema operativo. */
        return -1;                             /* Sale indicando fallo. */
    }                                          /* Fin de la validacion de apertura. */

    struct stat sb;                            /* Estructura que recibira metadata del archivo. */
    if (fstat(fd, &sb) == -1) {                /* Consulta tamano y atributos del archivo abierto. */
        perror("[ERR] fstat");                 /* Muestra el motivo del fallo de fstat. */
        close(fd);                             /* Cierra el descriptor para no dejar recursos abiertos. */
        return -1;                             /* Sale indicando error. */
    }                                          /* Fin de la validacion de fstat. */

    if (sb.st_size == 0) {                     /* Maneja el caso especial de archivo vacio. */
        fprintf(stderr, "[WARN] El archivo esta vacio.\n"); /* Informa que no hay registros por enviar. */
        close(fd);                             /* Cierra el archivo porque no se usara mas. */
        return 0;                              /* No es error fatal: simplemente no habia datos. */
    }                                          /* Fin del caso de archivo vacio. */

    char *addr = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0); /* Mapea todo el archivo en memoria virtual. */
    if (addr == MAP_FAILED) {                  /* Verifica si el mapeo fallo. */
        perror("[ERR] mmap");                  /* Informa la razon del fallo del mapeo. */
        close(fd);                             /* Cierra el descriptor del archivo. */
        return -1;                             /* Sale con error. */
    }                                          /* Fin de la validacion de mmap. */

    const char *cursor = addr;                 /* Cursor de lectura que recorrera el archivo mapeado. */
    const char *fin = addr + sb.st_size;       /* Puntero al byte siguiente al final del archivo. */
    long enviados = 0;                         /* Cuenta mensajes aceptados por el productor. */
    long fallidos = 0;                         /* Cuenta mensajes que no pudieron encolarse. */

    printf("[BULK] Procesando %ld bytes...\n", (long)sb.st_size); /* Informa el tamano de la carga a procesar. */

    while (cursor < fin && g_running) {        /* Recorre el archivo hasta el final o hasta cancelacion. */
        const char *nl = memchr(cursor, '\n', (size_t)(fin - cursor)); /* Busca el siguiente salto de linea. */
        if (nl == NULL) {                      /* Si no hay salto de linea, estamos en la ultima fila. */
            nl = fin;                          /* Usa el final del archivo como limite de la ultima linea. */
        }                                      /* Fin del ajuste para ultima linea. */

        size_t len = (size_t)(nl - cursor);    /* Calcula cuantos bytes tiene la linea actual. */
        if (len > 0 && cursor[len - 1] == '\r') { /* Corrige finales CRLF comunes en CSV de Windows. */
            len--;                             /* Excluye el retorno de carro del payload real. */
        }                                      /* Fin de la normalizacion de CRLF. */

        if (len > 0) {                         /* Ignora lineas completamente vacias. */
            if (enviar_linea(rk, "retail-bulk", cursor, len) == 0) { /* Intenta enviar la linea actual. */
                enviados++;                    /* Suma un mensaje aceptado por la cola del productor. */
            } else {                           /* Si la publicacion fracaso definitivamente... */
                fallidos++;                    /* Registra el fallo para el resumen final. */
            }                                  /* Fin del conteo por resultado. */
        }                                      /* Fin del filtrado de lineas vacias. */

        cursor = (nl < fin) ? nl + 1 : fin;    /* Avanza al inicio de la siguiente linea sin salir del rango. */

        if ((enviados + fallidos) % 10000 == 0) { /* Cada cierto numero de filas atendemos eventos de red. */
            rd_kafka_poll(rk, 0);              /* Ejecuta callbacks y progreso de IO sin bloquear. */
        }                                      /* Fin del sondeo periodico. */
    }                                          /* Fin del recorrido del archivo. */

    rd_kafka_flush(rk, 10000);                 /* Espera la entrega de mensajes pendientes antes de liberar recursos. */
    munmap(addr, sb.st_size);                  /* Desmapea el archivo ya procesado de la memoria virtual. */
    close(fd);                                 /* Cierra el descriptor del archivo. */

    printf("[BULK] Enviados=%ld, fallidos=%ld\n", enviados, fallidos); /* Resume el resultado final del proceso. */
    return (fallidos == 0) ? 0 : -1;           /* Devuelve exito solo si no hubo fallos. */
}                                              /* Fin de procesar_bulk_mmap. */

int main(int argc, char **argv) {              /* Punto de entrada del programa. */
    if (argc != 2) {                           /* Exige exactamente un argumento: la ruta del CSV. */
        fprintf(stderr, "Uso: %s <archivo_csv>\n", argv[0]); /* Informa la sintaxis correcta. */
        return EXIT_FAILURE;                   /* Finaliza con codigo de error por uso incorrecto. */
    }                                          /* Fin de la validacion de argumentos. */

    struct sigaction sa;                       /* Estructura recomendada para instalar senales de forma robusta. */
    memset(&sa, 0, sizeof(sa));                /* Inicializa todos sus campos en cero. */
    sa.sa_handler = on_stop;                   /* Registra nuestro manejador para SIGINT. */
    sigaction(SIGINT, &sa, NULL);              /* Instala el manejador para capturar Ctrl+C. */

    char errstr[512];                          /* Buffer para mensajes de error de configuracion de Kafka. */
    rd_kafka_conf_t *conf = rd_kafka_conf_new(); /* Crea un objeto de configuracion vacio. */

    if (rd_kafka_conf_set(conf, "bootstrap.servers", "kafka:29092", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) { /* Configura la direccion del broker. */
        fprintf(stderr, "[ERR] conf bootstrap.servers: %s\n", errstr); /* Reporta error de configuracion. */
        rd_kafka_conf_destroy(conf);           /* Libera la configuracion para evitar fuga de memoria. */
        return EXIT_FAILURE;                   /* Sale con error. */
    }                                          /* Fin de la configuracion del bootstrap. */

    rd_kafka_conf_set(conf, "client.id", "bulk-mmap-producer", NULL, 0); /* Asigna un identificador util para monitoreo. */
    rd_kafka_conf_set(conf, "linger.ms", "5", NULL, 0); /* Permite agrupar mensajes brevemente para mejorar throughput. */
    rd_kafka_conf_set(conf, "batch.num.messages", "50000", NULL, 0); /* Ajusta el tamano maximo del lote en numero de mensajes. */
    rd_kafka_conf_set(conf, "acks", "all", NULL, 0); /* Solicita confirmacion del lider y replicas ISR disponibles. */

    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)); /* Crea el productor Kafka. */
    if (rk == NULL) {                          /* Verifica si la creacion del productor fallo. */
        fprintf(stderr, "[ERR] rd_kafka_new: %s\n", errstr); /* Informa el error devuelto por librdkafka. */
        return EXIT_FAILURE;                   /* Sale con error de inicializacion. */
    }                                          /* Fin de la validacion del productor. */

    int rc = procesar_bulk_mmap(argv[1], rk);  /* Ejecuta la logica principal con el archivo indicado. */
    rd_kafka_destroy(rk);                      /* Libera completamente la instancia del productor. */
    return (rc == 0) ? EXIT_SUCCESS : EXIT_FAILURE; /* Devuelve al sistema el estado final del programa. */
}                                              
