# dados_sens_pthread

Análise de dados de sensores IoT utilizando pthreads (POSIX Threads) em C.

## Requisitos

- GCC
- biblioteca pthreads (normalmente já inclusa no Linux)

## Arquivos necessários

Descompacte a pasta "Dados.zip" e mova os arquivos internos para fora da pasta descompactada.

Os arquivos json descompactados devem conter os seguintes nomes:

- `mqtt_senzemo_cx_bg.json`
- `senzemo_cx_bg.json`

## Como compilar

```bash
gcc -o programa processamento_threads.c cJSON.c -lpthread -lm
```

## Como rodar

```bash
./programa
```

O programa vai processar os dois arquivos JSON em paralelo usando threads e exibir na tela um relatório com as estatísticas de temperatura, umidade, pressão atmosférica, bateria e spreading factors  .Após a execução, o arquivo `processamento.log` será gerado 
na mesma pasta com o registro de todas as operações.

## Configuração do VS Code

Adicione uma pasta `.vscode` com o arquivo `tasks.json` dentro dela para usar a tarefa de build integrada ao editor.
