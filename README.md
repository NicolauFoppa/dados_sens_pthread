Participantes: Nicolau Foppa e Vicenzo Vilhena Basso

# dados_sens_pthread

Análise de dados de sensores IoT utilizando pthreads (POSIX Threads) em C.

## Requisitos

- GCC
- biblioteca pthreads (normalmente já inclusa no Linux)

## Arquivos necessários

Descompacte o arquivo `Dados.zip` na mesma pasta onde estão os arquivos do projeto.
Os dois arquivos JSON necessários para a execução serão extraídos automaticamente.
Caso não sejam extraidos automaticamente na pasta com o restante dos arquivos, mova-os
para a mesma.

Os arquivos JSON devem ter os seguintes nomes:
- `mqtt_senzemo_cx_bg.json`
- `senzemo_cx_bg.json`

Caso os nomes estejam diferentes, renomeie-os antes de compilar.

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
