#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <time.h>

#define tamanho_lista 256

typedef struct{
    float id;
    char datetime[64];
    float temperatura;
    float umidade;
    float pressao;
    float bateria;
    int spreading_factor;
} Record;

typedef struct{
    Record lista[tamanho_lista];
    int head;
    int tail;
    int count;
    int done;
    pthread_mutex_t mutex;
    sem_t empty;
    sem_t full;
}Fila;

typedef struct{
    float temperaturaMin;
    char temperaturaMinData[64];
    float temperaturaMax;
    char temperaturaMaxData[64];
    float temperaturaSoma;

    float pressaoMin;
    char pressaoMinData[64];
    float pressaoMax;
    char pressaoMaxData[64];
    float pressaoSoma;

    float umidadeMin;
    char umidadeMinData[64];
    float umidadeMax;
    char umidadeMaxData[64];
    float umidadeSoma;

    float batInicial;
    float batFinal;

    int sfUsado[22];

    int total;
    char periodoInicio[64];
    char periodoFim[64];
}Stats;

#define LOG_QUEUE_SIZE 512

typedef struct {
    char msg[256];
} LogMsg;

typedef struct {
    LogMsg msgs[LOG_QUEUE_SIZE];
    int head, tail, done;
    pthread_mutex_t mutex;
    sem_t empty, full;
} LogQueue;

typedef struct{
    Stats stats;
    Fila fila;
    char nomeArquivo[64];
    LogQueue *log_q;
} Context;


void iniciaLogQueue(LogQueue *q) {
    q->head = q->tail = q->done = 0;
    pthread_mutex_init(&q->mutex, NULL);
    sem_init(&q->empty, 0, LOG_QUEUE_SIZE);
    sem_init(&q->full, 0, 0);
}

void addLog(LogQueue *q, const char *msg) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char buf[512];
    snprintf(buf, sizeof(buf), "[%04d-%02d-%02d %02d:%02d:%02d] %s",
             t->tm_year+1900, t->tm_mon+1, t->tm_mday,
             t->tm_hour, t->tm_min, t->tm_sec, msg);
    sem_wait(&q->empty);
    pthread_mutex_lock(&q->mutex);
    strncpy(q->msgs[q->tail].msg, buf, 255);
    q->msgs[q->tail].msg[255] = '\0';
    q->tail = (q->tail + 1) % LOG_QUEUE_SIZE;
    pthread_mutex_unlock(&q->mutex);
    sem_post(&q->full);
}

int removeLog(LogQueue *q, LogMsg *m) {
    sem_wait(&q->full);
    pthread_mutex_lock(&q->mutex);
    if (q->done && q->head == q->tail) {
        pthread_mutex_unlock(&q->mutex);
        return 0;
    }
    *m = q->msgs[q->head];
    q->head = (q->head + 1) % LOG_QUEUE_SIZE;
    pthread_mutex_unlock(&q->mutex);
    sem_post(&q->empty);
    return 1;
}

void terminaLog(LogQueue *q) {
    q->done = 1;
    sem_post(&q->full);
}

void *thread_log(void *arg) {
    LogQueue *q = (LogQueue *) arg;
    FILE *f = fopen("processamento.log", "w");
    if (!f) return NULL;
    LogMsg m;
    while (removeLog(q, &m))
        fprintf(f, "%s\n", m.msg);
    fclose(f);
    return NULL;
}

void iniciaFila(Fila *q){
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->done = 0;
    pthread_mutex_init(&q->mutex, NULL);
    sem_init(&q->empty, 0, tamanho_lista);
    sem_init(&q->full, 0, 0);
}

void addRecordToFila(Fila *fila, Record *r){
   sem_wait(&fila->empty);
   pthread_mutex_lock(&fila->mutex);
   fila->lista[fila->tail] = *r;
   fila->tail = (fila->tail + 1) % tamanho_lista;
   pthread_mutex_unlock(&fila->mutex);
   sem_post(&fila->full);
}

int removeRecord(Fila *fila, Record *r){
    sem_wait(&fila->full);
    pthread_mutex_lock(&fila->mutex);

    if(fila->done && fila->head == fila->tail){
        pthread_mutex_unlock(&fila->mutex);
        return 0;
    }
    *r = fila->lista[fila->head];
    fila->head = (fila->head + 1) % tamanho_lista;
    pthread_mutex_unlock(&fila->mutex);
    sem_post(&fila->empty);
    return 1;
}

void termina(Fila *fila){
    fila->done = 1;
    sem_post(&fila->full);
}

void *thread_leitura(void *args){
    Context *ctx = (Context *) args;
    char logbuf[256];

    snprintf(logbuf, sizeof(logbuf), "Thread leitura iniciada: %s", ctx->nomeArquivo);
    addLog(ctx->log_q, logbuf);

    FILE *file = fopen(ctx->nomeArquivo, "r");
    if(!file){
        snprintf(logbuf, sizeof(logbuf), "Erro: arquivo nao encontrado: %s", ctx->nomeArquivo);
        addLog(ctx->log_q, logbuf);
        return NULL;
    }

    fseek(file, 0, SEEK_END);
    long size = ftell(file);
    fseek(file, 0, SEEK_SET);

    char *content = malloc(size + 1);
    fread(content, 1, size, file);
    content[size] = '\0';
    fclose(file);

    cJSON *root = cJSON_Parse(content);
    free(content);

    cJSON *item = NULL;
    cJSON_ArrayForEach(item, root){
        cJSON *createdAt = cJSON_GetObjectItem(item, "created_at");
        if(!createdAt)
            createdAt = cJSON_GetObjectItem(item, "payload_date");

        cJSON *payloadStr = cJSON_GetObjectItem(item, "payload");
            if(!payloadStr)
            payloadStr = cJSON_GetObjectItem(item, "brute_data");

        if(!payloadStr || !createdAt) continue;

        cJSON *payload = cJSON_Parse(payloadStr->valuestring);
        if(!payload) continue;

        cJSON *data = cJSON_GetObjectItem(payload, "data");
        if(!data){cJSON_Delete(payload); continue; }

        Record r;

        strcpy(r.datetime, createdAt->valuestring);
        r.id = -999.0f;
        r.umidade = -999.0f;
        r.pressao = -999.0f;
        r.temperatura = -999.0f;
        r.bateria = -999.0f;
        r.spreading_factor = -1;

        cJSON *entry = NULL;
        cJSON_ArrayForEach(entry, data){
            cJSON *variable = cJSON_GetObjectItem(entry, "variable");
            cJSON *value = cJSON_GetObjectItem(entry, "value");
            if(!variable || !value) continue;

            if(strcmp(variable->valuestring, "temperature") == 0)
                r.temperatura = (float) value->valuedouble;
            else if(strcmp(variable->valuestring, "humidity") == 0)
                r.umidade = (float) value->valuedouble;
            else if(strcmp(variable->valuestring, "airpressure") == 0)
                r.pressao = (float) value->valuedouble;
            else if(strcmp(variable->valuestring, "batterylevel") == 0)
                r.bateria = (float) value->valuedouble;
            else if(strcmp(variable->valuestring, "lora_spreading_factor") == 0)
                r.spreading_factor = (int) value->valuedouble;

        }

        cJSON_Delete(payload);

        addRecordToFila(&ctx->fila, &r);

    }

    cJSON_Delete(root);
    snprintf(logbuf, sizeof(logbuf), "Thread leitura concluida: %s", ctx->nomeArquivo);
    addLog(ctx->log_q, logbuf);
    termina(&ctx->fila);
    return NULL;
}

void *thread_calculo(void *arg){
    Context *ctx = (Context *) arg;
    char logbuf[256];
    Record r;

    snprintf(logbuf, sizeof(logbuf), "Thread calculo iniciada: %s", ctx->nomeArquivo);
    addLog(ctx->log_q, logbuf);

    while(removeRecord(&ctx->fila, &r)){
        if(r.temperatura != -999.0f){
            if(r.temperatura < ctx->stats.temperaturaMin){
                ctx->stats.temperaturaMin = r.temperatura;
                strcpy(ctx->stats.temperaturaMinData, r.datetime);
            }
            if(r.temperatura > ctx->stats.temperaturaMax){
                ctx->stats.temperaturaMax = r.temperatura;
                strcpy(ctx->stats.temperaturaMaxData, r.datetime);
            }
            ctx->stats.temperaturaSoma += r.temperatura;
        }

        if(r.pressao != -999.0f){
            if(r.pressao < ctx->stats.pressaoMin){
                ctx->stats.pressaoMin = r.pressao;
                strcpy(ctx->stats.pressaoMinData, r.datetime);
            }
            if(r.pressao > ctx->stats.pressaoMax){
                ctx->stats.pressaoMax = r.pressao;
                strcpy(ctx->stats.pressaoMaxData, r.datetime);
            }
            ctx->stats.pressaoSoma += r.pressao;
        }

        if(r.umidade != -999.0f){
            if(r.umidade < ctx->stats.umidadeMin){
                ctx->stats.umidadeMin = r.umidade;
                strcpy(ctx->stats.umidadeMinData, r.datetime);
            }
            if(r.umidade > ctx->stats.umidadeMax){
                ctx->stats.umidadeMax = r.umidade;
                strcpy(ctx->stats.umidadeMaxData, r.datetime);
            }
            ctx->stats.umidadeSoma += r.umidade;
        }

        if(ctx->stats.total == 0){
            ctx->stats.batInicial = r.bateria;
            strncpy(ctx->stats.periodoInicio, r.datetime, 63);
        }
        ctx->stats.batFinal = r.bateria;
        strncpy(ctx->stats.periodoFim, r.datetime, 63);

        if(r.spreading_factor > -1){
            ctx->stats.sfUsado[r.spreading_factor] = 1;
        }

        ctx->stats.total++;
    }

    snprintf(logbuf, sizeof(logbuf), "Thread calculo concluida: %s - %d registros", ctx->nomeArquivo, ctx->stats.total);
    addLog(ctx->log_q, logbuf);

    return NULL;
}

static void printPad(const char *s, int w) {
    printf("%s", s);
    int bytes = (int)strlen(s), vis = 0;
    for (int i = 0; i < bytes; ) {
        unsigned char c = (unsigned char)s[i];
        if      (c < 0x80) { vis++; i += 1; }
        else if (c < 0xE0) { vis++; i += 2; }
        else if (c < 0xF0) { vis++; i += 3; }
        else               { vis++; i += 4; }
    }
    for (int i = vis; i < w; i++) printf(" ");
}

static void formatarData(const char *in, char *out, size_t len) {
    int y, mo, d, h, mi, s;
    if (sscanf(in, "%4d-%2d-%2dT%2d:%2d:%2d", &y, &mo, &d, &h, &mi, &s) == 6 ||
        sscanf(in, "%4d-%2d-%2d %2d:%2d:%2d", &y, &mo, &d, &h, &mi, &s) == 6)
        snprintf(out, len, "%02d/%02d/%04d %02d:%02d:%02d", d, mo, y, h, mi, s);
    else { strncpy(out, in, len-1); out[len-1] = '\0'; }
}

static void formatarDataCurta(const char *in, char *out, size_t len) {
    int y, mo, d;
    if (sscanf(in, "%4d-%2d-%2d", &y, &mo, &d) == 3)
        snprintf(out, len, "%02d/%02d/%04d", d, mo, y);
    else { strncpy(out, in, len-1); out[len-1] = '\0'; }
}

static void printIntBR(int n) {
    if (n >= 1000000)
        printf("%d.%03d.%03d", n/1000000, (n/1000)%1000, n%1000);
    else if (n >= 1000)
        printf("%d.%03d", n/1000, n%1000);
    else
        printf("%d", n);
}

void printLinha(int tipo){
    if(tipo == 0) {printf("============================================================\n"); return;}
    if(tipo == 1) {printf("------------------------------------------------------------\n"); return;}
    printf("-----------------------------------------------------------------------------------------------\n");
}

void printResultados(Context *ctx_caxias, Context *ctx_bento, double tempo) {
    Stats *sc = &ctx_caxias->stats;
    Stats *sb = &ctx_bento->stats;
    char d1[32], d2[32], pi[32], pf[32];

    printLinha(0);
    printf("ANÁLISE DE DADOS DOS SENSORES - CityLivingLab\n");
    printf("Processamento utilizando pthreads\n");
    printLinha(0);
    printf("\n");

    formatarDataCurta(sc->periodoInicio, pi, sizeof(pi));
    formatarDataCurta(sc->periodoFim,    pf, sizeof(pf));
    printf("Arquivo analisado: %s\n", ctx_caxias->nomeArquivo);
    printf("Total de registros processados: "); printIntBR(sc->total); printf("\n");
    printf("Período analisado: %s a %s\n", pi, pf);
    printf("\n");

    formatarDataCurta(sb->periodoInicio, pi, sizeof(pi));
    formatarDataCurta(sb->periodoFim,    pf, sizeof(pf));
    printf("Arquivo analisado: %s\n", ctx_bento->nomeArquivo);
    printf("Total de registros processados: "); printIntBR(sb->total); printf("\n");
    printf("Período analisado: %s a %s\n", pi, pf);
    printf("\n");

    // TEMPERATURA
    printLinha(1);
    printf("TEMPERATURA (°C)\n");
    printLinha(1);
    printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
    printLinha(2);
    formatarData(sc->temperaturaMinData, d1, sizeof(d1));
    formatarData(sc->temperaturaMaxData, d2, sizeof(d2));
    printPad("Caxias do Sul", 18);
    printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
           sc->temperaturaMin, d1, sc->temperaturaMax, d2,
           sc->temperaturaSoma / sc->total);
    formatarData(sb->temperaturaMinData, d1, sizeof(d1));
    formatarData(sb->temperaturaMaxData, d2, sizeof(d2));
    printPad("Bento Gonçalves", 18);
    printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
           sb->temperaturaMin, d1, sb->temperaturaMax, d2,
           sb->temperaturaSoma / sb->total);
    printf("\n\n");

    // UMIDADE
    printLinha(1);
    printf("UMIDADE (%%)\n");
    printLinha(1);
    printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
    printLinha(2);
    formatarData(sc->umidadeMinData, d1, sizeof(d1));
    formatarData(sc->umidadeMaxData, d2, sizeof(d2));
    printPad("Caxias do Sul", 18);
    printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
           sc->umidadeMin, d1, sc->umidadeMax, d2,
           sc->umidadeSoma / sc->total);
    formatarData(sb->umidadeMinData, d1, sizeof(d1));
    formatarData(sb->umidadeMaxData, d2, sizeof(d2));
    printPad("Bento Gonçalves", 18);
    printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
           sb->umidadeMin, d1, sb->umidadeMax, d2,
           sb->umidadeSoma / sb->total);
    printf("\n\n");

    // PRESSÃO ATMOSFÉRICA
    printLinha(1);
    printf("PRESSÃO ATMOSFÉRICA (hPa)\n");
    printLinha(1);
    printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
    printLinha(2);
    formatarData(sc->pressaoMinData, d1, sizeof(d1));
    formatarData(sc->pressaoMaxData, d2, sizeof(d2));
    printPad("Caxias do Sul", 18);
    printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
           sc->pressaoMin, d1, sc->pressaoMax, d2,
           sc->pressaoSoma / sc->total);
    formatarData(sb->pressaoMinData, d1, sizeof(d1));
    formatarData(sb->pressaoMaxData, d2, sizeof(d2));
    printPad("Bento Gonçalves", 18);
    printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
           sb->pressaoMin, d1, sb->pressaoMax, d2,
           sb->pressaoSoma / sb->total);
    printf("\n\n");

    // BATERIA
    printLinha(1);
    printf("BATERIA\n");
    printLinha(1);
    printf("%-18s| %-12s| %-10s| %s\n", "Cidade", "Inicial (V)", "Final (V)", "Consumo (V)");
    printLinha(1);
    printPad("Caxias do Sul", 18);
    printf("| %-12.2f| %-10.2f| %.2f\n",
           sc->batInicial, sc->batFinal, sc->batInicial - sc->batFinal);
    printPad("Bento Gonçalves", 18);
    printf("| %-12.2f| %-10.2f| %.2f\n",
           sb->batInicial, sb->batFinal, sb->batInicial - sb->batFinal);
    printf("\n\n");

    // SPREADING FACTORS
    printLinha(1);
    printf("SPREADING FACTORS UTILIZADOS\n");
    printLinha(1);
    printf("%-18s| %s\n", "Cidade", "SF utilizados");
    printLinha(1);
    printPad("Caxias do Sul", 18);
    printf("| ");
    { int first = 1;
      for (int i = 0; i < 22; i++) {
          if (sc->sfUsado[i]) { if (!first) printf(", "); printf("SF%d", i); first = 0; }
      }
      if (first) printf("nenhum");
    }
    printf("\n");
    printPad("Bento Gonçalves", 18);
    printf("| ");
    { int first = 1;
      for (int i = 0; i < 22; i++) {
          if (sb->sfUsado[i]) { if (!first) printf(", "); printf("SF%d", i); first = 0; }
      }
      if (first) printf("nenhum");
    }
    printf("\n");
    printf("\n\n");

    // DESEMPENHO
    printLinha(1);
    printf("DESEMPENHO\n");
    printLinha(1);
    printf("Tempo total de execução: %.2f segundos\n", tempo);
    printf("Threads utilizadas: 3\n");
    printf(" - Thread 1: leitura dos dados\n");
    printf(" - Thread 2: cálculo das estatísticas\n");
    printf(" - Thread 3: registro de logs\n");
    printf("\n");
    printf("Arquivo de log gerado: processamento.log\n");
    printf("\n");
    printLinha(0);
    printf("Processamento finalizado com sucesso.\n");
    printLinha(0);
}


int main() {
    struct timespec t_inicio, t_fim;
    clock_gettime(CLOCK_MONOTONIC, &t_inicio);

    Context ctx_caxias;
    Context ctx_bento;
    memset(&ctx_caxias, 0, sizeof(Context));
    memset(&ctx_bento,  0, sizeof(Context));

    LogQueue log_q;
    iniciaLogQueue(&log_q);
    ctx_caxias.log_q = &log_q;
    ctx_bento.log_q  = &log_q;

    strncpy(ctx_caxias.nomeArquivo, "sensores_caxias.json", 63);
    strncpy(ctx_bento.nomeArquivo,  "sensores_bento.json",  63);

    iniciaFila(&ctx_caxias.fila);
    iniciaFila(&ctx_bento.fila);

    ctx_caxias.stats.temperaturaMin = FLT_MAX;
    ctx_caxias.stats.temperaturaMax = -FLT_MAX;
    ctx_caxias.stats.umidadeMin = FLT_MAX;
    ctx_caxias.stats.umidadeMax = -FLT_MAX;
    ctx_caxias.stats.pressaoMin = FLT_MAX;
    ctx_caxias.stats.pressaoMax = -FLT_MAX;

    ctx_bento.stats.temperaturaMin = FLT_MAX;
    ctx_bento.stats.temperaturaMax = -FLT_MAX;
    ctx_bento.stats.umidadeMin = FLT_MAX;
    ctx_bento.stats.umidadeMax = -FLT_MAX;
    ctx_bento.stats.pressaoMin = FLT_MAX;
    ctx_bento.stats.pressaoMax = -FLT_MAX;

    pthread_t t_log;
    pthread_t t_leitura_caxias, t_calculo_caxias;
    pthread_t t_leitura_bento,  t_calculo_bento;

    pthread_create(&t_log,            NULL, thread_log,     &log_q);
    pthread_create(&t_leitura_caxias, NULL, thread_leitura, &ctx_caxias);
    pthread_create(&t_calculo_caxias, NULL, thread_calculo, &ctx_caxias);
    pthread_create(&t_leitura_bento,  NULL, thread_leitura, &ctx_bento);
    pthread_create(&t_calculo_bento,  NULL, thread_calculo, &ctx_bento);

    pthread_join(t_leitura_caxias, NULL);
    pthread_join(t_calculo_caxias, NULL);
    pthread_join(t_leitura_bento,  NULL);
    pthread_join(t_calculo_bento,  NULL);

    terminaLog(&log_q);
    pthread_join(t_log, NULL);

    clock_gettime(CLOCK_MONOTONIC, &t_fim);
    double tempo = (t_fim.tv_sec  - t_inicio.tv_sec) +
                   (t_fim.tv_nsec - t_inicio.tv_nsec) / 1e9;

    printResultados(&ctx_caxias, &ctx_bento, tempo);
    return 0;
}
