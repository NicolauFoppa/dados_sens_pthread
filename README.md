# dados_sens_pthread

Análise de dados de sensores IoT utilizando pthreads (POSIX Threads) em C.

## Requisitos

- GCC
- biblioteca pthreads (normalmente já inclusa no Linux)

## Arquivos necessários

Coloque os arquivos JSON dos sensores no mesmo diretório do programa:

- `sensores_caxias.json` — dados de Caxias do Sul
- `sensores_bento.json` — dados de Bento Gonçalves

## Como compilar

```bash
gcc -o programa processamento_threads.c cJSON.c -lpthread -lm
```

## Como rodar

```bash
./programa
```

O programa vai processar os dois arquivos JSON em paralelo usando threads e exibir na tela um relatório com as estatísticas de temperatura, umidade, pressão atmosférica, bateria e spreading factors. Também gera o arquivo `processamento.log` com o registro das operações.

## Configuração do VS Code

Adicione uma pasta `.vscode` com o arquivo `tasks.json` dentro dela para usar a tarefa de build integrada ao editor.
