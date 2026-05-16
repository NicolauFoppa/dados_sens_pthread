#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include "cJSON.h"

#define TAMANHO_FILA   256
#define HASH_SIZE 65536
#define LOG_QUEUE_SIZE 512

int *hashInit() {
	return calloc(HASH_SIZE, sizeof(int));
}

int verificaDuplicado(int *tabela, int id) {
	int pos = id % HASH_SIZE;   
	if (tabela[pos] == id && id != 0)
		return 1;
	tabela[pos] = id;
	return 0;
}

void hashFree(int *tabela) {
	free(tabela);
}

typedef struct {
	char dateTime[64];
	float temperatura;
	float umidade;
	float pressao;
	float bateria;
	int spreadingFactor;
	int hasTemperatura;
	int hasUmidade;
	int hasPressao;
	int hasBateria;
	int hasSpreadingFactor;
} Record;

typedef struct {
	Record lista[TAMANHO_FILA];
	int head;
	int tail;
	int done;
	pthread_mutex_t mutex;
	sem_t empty;
	sem_t full;
} Fila;

typedef struct {
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
	char batInicialData[64];
	char batFinalData[64];
	int batInicialSet;
	int batFinalSet;

	int sfUsado[22];
	int total;

	char periodoInicio[64];
	char periodoFim[64];
} Stats;

typedef struct {
	char msg[256];
} LogMsg;

typedef struct {
	LogMsg msgs[LOG_QUEUE_SIZE];
	int head;
	int tail;
	int done;
	pthread_mutex_t mutex;
	sem_t empty;
	sem_t full;
} LogQueue;

typedef struct {
	Stats stats;
	Fila fila;
	LogQueue *logQueue;
	int *hashTable;
} Context;

typedef struct {
	Context *caxias;
	Context *bento;
	char nomeArquivo[64];
} LeitorArgs;

void logQueueInit(LogQueue *q) {
	q->head = q->tail = q->done = 0;
	pthread_mutex_init(&q->mutex, NULL);
	sem_init(&q->empty, 0, LOG_QUEUE_SIZE);
	sem_init(&q->full, 0, 0);
}

void logAdd(LogQueue *q, const char *msg) {
	time_t now = time(NULL);
	struct tm *t = localtime(&now);
	char buf[512];
	snprintf(buf, sizeof(buf), "[%04d-%02d-%02d %02d:%02d:%02d] %s",
	         t->tm_year + 1900, t->tm_mon + 1, t->tm_mday,
	         t->tm_hour, t->tm_min, t->tm_sec, msg);
	sem_wait(&q->empty);
	pthread_mutex_lock(&q->mutex);
	strcpy(q->msgs[q->tail].msg, buf);
	q->msgs[q->tail].msg[255] = '\0';
	q->tail = (q->tail + 1) % LOG_QUEUE_SIZE;
	pthread_mutex_unlock(&q->mutex);
	sem_post(&q->full);
}

int logRemove(LogQueue *q, LogMsg *m) {
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

void logQueueTermina(LogQueue *q) {
	q->done = 1;
	sem_post(&q->full);
}

void filaInit(Fila *q) {
	q->head = 0;
	q->tail = 0;
	q->done = 0;
	pthread_mutex_init(&q->mutex, NULL);
	sem_init(&q->empty, 0, TAMANHO_FILA);
	sem_init(&q->full, 0, 0);
}

void filaAdd(Fila *fila, Record *r) {
	sem_wait(&fila->empty);
	pthread_mutex_lock(&fila->mutex);
	fila->lista[fila->tail] = *r;
	fila->tail = (fila->tail + 1) % TAMANHO_FILA;
	pthread_mutex_unlock(&fila->mutex);
	sem_post(&fila->full);
}

int filaRemove(Fila *fila, Record *r) {
	sem_wait(&fila->full);
	pthread_mutex_lock(&fila->mutex);
	if (fila->done && fila->head == fila->tail) {
		pthread_mutex_unlock(&fila->mutex);
		return 0;
	}
	*r = fila->lista[fila->head];
	fila->head = (fila->head + 1) % TAMANHO_FILA;
	pthread_mutex_unlock(&fila->mutex);
	sem_post(&fila->empty);
	return 1;
}

void filaTermina(Fila *fila) {
	fila->done = 1;
	sem_post(&fila->full);
}

void *threadLog(void *arg) {
	LogQueue *q = (LogQueue *) arg;
	FILE *f = fopen("processamento.log", "w");
	if (!f) {
		return NULL;
	}
	LogMsg m;
	while (logRemove(q, &m)) {
		fprintf(f, "%s\n", m.msg);
	}
	fclose(f);
	return NULL;
}

void *leArquivo(void *args) {
	LeitorArgs *largs = (LeitorArgs *) args;
	LogQueue *logQ = largs->caxias->logQueue;
	char logBuf[256];

	snprintf(logBuf, sizeof(logBuf), "Thread leitura iniciada: %s", largs->nomeArquivo);
	logAdd(logQ, logBuf);

	FILE *file = fopen(largs->nomeArquivo, "r");
	if (!file) {
		snprintf(logBuf, sizeof(logBuf), "Erro: arquivo nao encontrado: %s", largs->nomeArquivo);
		logAdd(logQ, logBuf);
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
	cJSON_ArrayForEach(item, root) {
		cJSON *createdAt = cJSON_GetObjectItem(item, "created_at");
		if (!createdAt) {
			createdAt = cJSON_GetObjectItem(item, "payload_date");
		}

		cJSON *idJson = cJSON_GetObjectItem(item, "id");
		if (!idJson) {
			idJson = cJSON_GetObjectItem(item, "payload_id");
		}
		if (!idJson) {
			continue;
		}
		int id = idJson->valueint;

		cJSON *payloadStr = cJSON_GetObjectItem(item, "payload");
		if (!payloadStr) {
			payloadStr = cJSON_GetObjectItem(item, "brute_data");
		}
		if (!payloadStr || !createdAt) {
			continue;
		}

		cJSON *payload = cJSON_Parse(payloadStr->valuestring);
		if (!payload) {
			continue;
		}

		cJSON *data = cJSON_GetObjectItem(payload, "data");
		if (!data) {
			cJSON_Delete(payload);
			continue;
		}

		cJSON *deviceName = cJSON_GetObjectItem(payload, "device_name");
		if (!deviceName) {
			cJSON_Delete(payload);
			continue;
		}

		Context *ctx;
		if (strstr(deviceName->valuestring, "Caxias") != NULL) {
			ctx = largs->caxias;
		} else if (strstr(deviceName->valuestring, "Bento") != NULL) {
			ctx = largs->bento;
		} else {
			cJSON_Delete(payload);
			continue;
		}

		if (verificaDuplicado(ctx->hashTable, id)) {
			cJSON_Delete(payload);
			continue;
		}

		Record r;
		strcpy(r.dateTime, createdAt->valuestring);
		r.temperatura = 0.0f;
		r.umidade = 0.0f;
		r.pressao = 0.0f;
		r.bateria = 0.0f;
		r.spreadingFactor = 0;
		r.hasTemperatura = 0;
		r.hasUmidade = 0;
		r.hasPressao = 0;
		r.hasBateria = 0;
		r.hasSpreadingFactor = 0;

		cJSON *entry = NULL;
		cJSON_ArrayForEach(entry, data) {
			cJSON *variable = cJSON_GetObjectItem(entry, "variable");
			cJSON *value = cJSON_GetObjectItem(entry, "value");
			if (!variable || !value) {
				continue;
			}
			if (strcmp(variable->valuestring, "temperature") == 0) {
				r.temperatura = (float) value->valuedouble;
				r.hasTemperatura = 1;
			} else if (strcmp(variable->valuestring, "humidity") == 0) {
				r.umidade = (float) value->valuedouble;
				r.hasUmidade = 1;
			} else if (strcmp(variable->valuestring, "airpressure") == 0) {
				r.pressao = (float) value->valuedouble;
				r.hasPressao = 1;
			} else if (strcmp(variable->valuestring, "batterylevel") == 0) {
				r.bateria = (float) value->valuedouble;
				r.hasBateria = 1;
			} else if (strcmp(variable->valuestring, "lora_spreading_factor") == 0) {
				r.spreadingFactor = (int) value->valuedouble;
				r.hasSpreadingFactor = 1;
			}
		}

		cJSON_Delete(payload);
		filaAdd(&ctx->fila, &r);
	}

	cJSON_Delete(root);
	snprintf(logBuf, sizeof(logBuf), "Thread leitura concluida: %s", largs->nomeArquivo);
	logAdd(logQ, logBuf);
	return NULL;
}

void *calculaStats(void *arg) {
	Context *ctx = (Context *) arg;
	char logBuf[256];
	Record r;

	snprintf(logBuf, sizeof(logBuf), "Thread calculo iniciada");
	logAdd(ctx->logQueue, logBuf);

	while (filaRemove(&ctx->fila, &r)) {
		if (r.hasTemperatura) {
			if (r.temperatura < ctx->stats.temperaturaMin) {
				ctx->stats.temperaturaMin = r.temperatura;
				strcpy(ctx->stats.temperaturaMinData, r.dateTime);
			}
			if (r.temperatura > ctx->stats.temperaturaMax) {
				ctx->stats.temperaturaMax = r.temperatura;
				strcpy(ctx->stats.temperaturaMaxData, r.dateTime);
			}
			ctx->stats.temperaturaSoma += r.temperatura;
		}

		if (r.hasPressao) {
			if (r.pressao < ctx->stats.pressaoMin) {
				ctx->stats.pressaoMin = r.pressao;
				strcpy(ctx->stats.pressaoMinData, r.dateTime);
			}
			if (r.pressao > ctx->stats.pressaoMax) {
				ctx->stats.pressaoMax = r.pressao;
				strcpy(ctx->stats.pressaoMaxData, r.dateTime);
			}
			ctx->stats.pressaoSoma += r.pressao;
		}

		if (r.hasUmidade) {
			if (r.umidade < ctx->stats.umidadeMin) {
				ctx->stats.umidadeMin = r.umidade;
				strcpy(ctx->stats.umidadeMinData, r.dateTime);
			}
			if (r.umidade > ctx->stats.umidadeMax) {
				ctx->stats.umidadeMax = r.umidade;
				strcpy(ctx->stats.umidadeMaxData, r.dateTime);
			}
			ctx->stats.umidadeSoma += r.umidade;
		}

		if (r.hasBateria) {
			if (!ctx->stats.batInicialSet || strcmp(r.dateTime, ctx->stats.batInicialData) < 0) {
				ctx->stats.batInicial = r.bateria;
				strcpy(ctx->stats.batInicialData, r.dateTime);
				ctx->stats.batInicialSet = 1;
			}
			if (!ctx->stats.batFinalSet || strcmp(r.dateTime, ctx->stats.batFinalData) > 0) {
				ctx->stats.batFinal = r.bateria;
				strcpy(ctx->stats.batFinalData, r.dateTime);
				ctx->stats.batFinalSet = 1;
			}
		}

		if (r.hasSpreadingFactor) {
			ctx->stats.sfUsado[r.spreadingFactor] = 1;
		}

		if (ctx->stats.periodoInicio[0] == '\0' || strcmp(r.dateTime, ctx->stats.periodoInicio) < 0) {
			strcpy(ctx->stats.periodoInicio, r.dateTime);
		}
		if (ctx->stats.periodoFim[0] == '\0' || strcmp(r.dateTime, ctx->stats.periodoFim) > 0) {
			strcpy(ctx->stats.periodoFim, r.dateTime);
		}

		ctx->stats.total++;
	}

	snprintf(logBuf, sizeof(logBuf), "Thread calculo concluida: %d registros", ctx->stats.total);
	logAdd(ctx->logQueue, logBuf);
	return NULL;
}

static void printPad(const char *s, int w) {
	int tam = strlen(s);
	printf("%s", s);
	for (int i = tam; i < w; i++) {
		printf(" ");
	}
}

static void formatarData(const char *in, char *out, size_t len) {
	int y, mo, d, h, mi, s;
	if (sscanf(in, "%4d-%2d-%2dT%2d:%2d:%2d", &y, &mo, &d, &h, &mi, &s) == 6 ||
	        sscanf(in, "%4d-%2d-%2d %2d:%2d:%2d", &y, &mo, &d, &h, &mi, &s) == 6) {
		snprintf(out, len, "%02d/%02d/%04d %02d:%02d:%02d", d, mo, y, h, mi, s);
	} else {
		strcpy(out, in);
		out[len - 1] = '\0';
	}
}

static void formatarDataCurta(const char *in, char *out, size_t len) {
	int y, mo, d;
	if (sscanf(in, "%4d-%2d-%2d", &y, &mo, &d) == 3) {
		snprintf(out, len, "%02d/%02d/%04d", d, mo, y);
	} else {
		strcpy(out, in);
		out[len - 1] = '\0';
	}
}

static void printIntBR(int n) {
	if (n >= 1000000) {
		printf("%d.%03d.%03d", n / 1000000, (n / 1000) % 1000, n % 1000);
	} else if (n >= 1000) {
		printf("%d.%03d", n / 1000, n % 1000);
	} else {
		printf("%d", n);
	}
}

void printResultados(LeitorArgs *arquivo1, LeitorArgs *arquivo2, double tempo) {
	Stats *sc = &arquivo1->caxias->stats;
	Stats *sb = &arquivo2->bento->stats;
	char d1[32], d2[32], pi[32], pf[32];

	printf("============================================================\n");
	printf("ANÁLISE DE DADOS DOS SENSORES - CityLivingLab\n");
	printf("Processamento utilizando pthreads\n");
	printf("============================================================\n");
	printf("\n");

	formatarDataCurta(sc->periodoInicio, pi, sizeof(pi));
	formatarDataCurta(sc->periodoFim, pf, sizeof(pf));
	printf("Arquivo analisado: %s\n", arquivo1->nomeArquivo);
	printf("Total de registros processados: ");
	printIntBR(sc->total);
	printf("\n");
	printf("Período analisado: %s a %s\n", pi, pf);
	printf("\n");

	formatarDataCurta(sb->periodoInicio, pi, sizeof(pi));
	formatarDataCurta(sb->periodoFim, pf, sizeof(pf));
	printf("Arquivo analisado: %s\n", arquivo2->nomeArquivo);
	printf("Total de registros processados: ");
	printIntBR(sb->total);
	printf("\n");
	printf("Período analisado: %s a %s\n", pi, pf);
	printf("\n");

	printf("------------------------------------------------------------\n");
	printf("TEMPERATURA (°C)\n");
	printf("------------------------------------------------------------\n");
	printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
	printf("-----------------------------------------------------------------------------------------------\n");
	formatarData(sc->temperaturaMinData, d1, sizeof(d1));
	formatarData(sc->temperaturaMaxData, d2, sizeof(d2));
	printPad("Caxias do Sul", 18);
	printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
	       sc->temperaturaMin, d1, sc->temperaturaMax, d2, sc->temperaturaSoma / sc->total);
	formatarData(sb->temperaturaMinData, d1, sizeof(d1));
	formatarData(sb->temperaturaMaxData, d2, sizeof(d2));
	printPad("Bento Gonçalves", 18);
	printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
	       sb->temperaturaMin, d1, sb->temperaturaMax, d2, sb->temperaturaSoma / sb->total);
	printf("\n\n");

	printf("------------------------------------------------------------\n");
	printf("UMIDADE (%%)\n");
	printf("------------------------------------------------------------\n");
	printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
	printf("-----------------------------------------------------------------------------------------------\n");
	formatarData(sc->umidadeMinData, d1, sizeof(d1));
	formatarData(sc->umidadeMaxData, d2, sizeof(d2));
	printPad("Caxias do Sul", 18);
	printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
	       sc->umidadeMin, d1, sc->umidadeMax, d2, sc->umidadeSoma / sc->total);
	formatarData(sb->umidadeMinData, d1, sizeof(d1));
	formatarData(sb->umidadeMaxData, d2, sizeof(d2));
	printPad("Bento Gonçalves", 18);
	printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
	       sb->umidadeMin, d1, sb->umidadeMax, d2, sb->umidadeSoma / sb->total);
	printf("\n\n");

	printf("------------------------------------------------------------\n");
	printf("PRESSÃO ATMOSFÉRICA (hPa)\n");
	printf("------------------------------------------------------------\n");
	printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
	printf("-----------------------------------------------------------------------------------------------\n");
	formatarData(sc->pressaoMinData, d1, sizeof(d1));
	formatarData(sc->pressaoMaxData, d2, sizeof(d2));
	printPad("Caxias do Sul", 18);
	printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
	       sc->pressaoMin, d1, sc->pressaoMax, d2, sc->pressaoSoma / sc->total);
	formatarData(sb->pressaoMinData, d1, sizeof(d1));
	formatarData(sb->pressaoMaxData, d2, sizeof(d2));
	printPad("Bento Gonçalves", 18);
	printf("| %-7.2f| %-22s| %-7.2f| %-22s| %.2f\n",
	       sb->pressaoMin, d1, sb->pressaoMax, d2, sb->pressaoSoma / sb->total);
	printf("\n\n");

	printf("------------------------------------------------------------\n");
	printf("BATERIA\n");
	printf("------------------------------------------------------------\n");
	printf("%-18s| %-12s| %-10s| %s\n", "Cidade", "Inicial (V)", "Final (V)", "Consumo (V)");
	printf("------------------------------------------------------------\n");
	printPad("Caxias do Sul", 18);
	printf("| %-12.2f| %-10.2f| %.2f\n", sc->batInicial, sc->batFinal, sc->batInicial - sc->batFinal);
	printPad("Bento Gonçalves", 18);
	printf("| %-12.2f| %-10.2f| %.2f\n", sb->batInicial, sb->batFinal, sb->batInicial - sb->batFinal);
	printf("\n\n");

	printf("------------------------------------------------------------\n");
	printf("SPREADING FACTORS UTILIZADOS\n");
	printf("------------------------------------------------------------\n");
	printf("%-18s| %s\n", "Cidade", "SF utilizados");
	printf("------------------------------------------------------------\n");
	printPad("Caxias do Sul", 18);
	printf("| ");
	int first = 1;
	for (int i = 0; i < 22; i++) {
		if (sc->sfUsado[i]) {
			if (!first) {
				printf(", ");
			}
			printf("SF%d", i);
			first = 0;
		}
	}
	if (first) {
		printf("nenhum");
	}
	printf("\n");

	printPad("Bento Gonçalves", 18);
	printf("| ");
	first = 1;
	for (int i = 0; i < 22; i++) {
		if (sb->sfUsado[i]) {
			if (!first) {
				printf(", ");
			}
			printf("SF%d", i);
			first = 0;
		}
	}
	if (first) {
		printf("nenhum");
	}
	printf("\n\n\n");

	printf("------------------------------------------------------------\n");
	printf("DESEMPENHO\n");
	printf("------------------------------------------------------------\n");
	printf("Tempo total de execução: %.2f segundos\n", tempo);
	printf("Threads utilizadas: 5\n");
	printf(" - Thread 1: leitura dos dados do arquivo: %s\n", arquivo1->nomeArquivo);
	printf(" - Thread 2: leitura dos dados do arquivo: %s\n", arquivo2->nomeArquivo);
	printf(" - Thread 3: cálculo das estatísticas (Caxias do Sul)\n");
	printf(" - Thread 4: cálculo das estatísticas (Bento Gonçalves)\n");
	printf(" - Thread 5: registro de logs\n");
	printf("\n");
	printf("Arquivo de log gerado: processamento.log\n");
	printf("\n");
	printf("============================================================\n");
	printf("Processamento finalizado com sucesso.\n");
	printf("============================================================\n");
}

int main() {
	struct timespec tInicio, tFim;
	clock_gettime(CLOCK_MONOTONIC, &tInicio);

	Context ctxCaxias;
	Context ctxBento;
	memset(&ctxCaxias, 0, sizeof(Context));
	memset(&ctxBento, 0, sizeof(Context));

	LogQueue logQ;
	logQueueInit(&logQ);
	ctxCaxias.logQueue = &logQ;
	ctxBento.logQueue = &logQ;

	filaInit(&ctxCaxias.fila);
	filaInit(&ctxBento.fila);

	ctxCaxias.stats.temperaturaMin = FLT_MAX;
	ctxCaxias.stats.temperaturaMax = -FLT_MAX;
	ctxCaxias.stats.umidadeMin = FLT_MAX;
	ctxCaxias.stats.umidadeMax = -FLT_MAX;
	ctxCaxias.stats.pressaoMin = FLT_MAX;
	ctxCaxias.stats.pressaoMax = -FLT_MAX;

	ctxBento.stats.temperaturaMin = FLT_MAX;
	ctxBento.stats.temperaturaMax = -FLT_MAX;
	ctxBento.stats.umidadeMin = FLT_MAX;
	ctxBento.stats.umidadeMax = -FLT_MAX;
	ctxBento.stats.pressaoMin = FLT_MAX;
	ctxBento.stats.pressaoMax = -FLT_MAX;

	ctxCaxias.hashTable = hashInit();
	ctxBento.hashTable  = hashInit();

	LeitorArgs argsArquivo1 = { &ctxCaxias, &ctxBento, "mqtt_senzemo_cx_bg.json" };
	LeitorArgs argsArquivo2 = { &ctxCaxias, &ctxBento, "senzemo_cx_bg.json" };

	pthread_t tLog;
	pthread_t tLeituraCaxias, tCalculoCaxias;
	pthread_t tLeituraBento, tCalculoBento;

	pthread_create(&tLog, NULL, threadLog, &logQ);
	pthread_create(&tLeituraCaxias, NULL, leArquivo, &argsArquivo1);
	pthread_create(&tLeituraBento, NULL, leArquivo, &argsArquivo2);
	pthread_create(&tCalculoCaxias, NULL, calculaStats, &ctxCaxias);
	pthread_create(&tCalculoBento, NULL, calculaStats, &ctxBento);

	pthread_join(tLeituraCaxias, NULL);
	pthread_join(tLeituraBento, NULL);

	filaTermina(&ctxCaxias.fila);
	filaTermina(&ctxBento.fila);

	pthread_join(tCalculoCaxias, NULL);
	pthread_join(tCalculoBento, NULL);

	logQueueTermina(&logQ);
	pthread_join(tLog, NULL);

	clock_gettime(CLOCK_MONOTONIC, &tFim);
	double tempo = (tFim.tv_sec - tInicio.tv_sec) +
	               (tFim.tv_nsec - tInicio.tv_nsec) / 1e9;

	printResultados(&argsArquivo1, &argsArquivo2, tempo);

	hashFree(ctxCaxias.hashTable);
	hashFree(ctxBento.hashTable);

	return 0;
}
