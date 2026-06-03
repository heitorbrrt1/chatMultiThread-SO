/*
 * servidor.cpp — Servidor de Chat Multicliente
 *
 * Disciplina: Sistemas Operacionais — IFG Campus Anápolis
 * Compilar e executar:
 *   g++ -pthread servidor.cpp -o servidor && ./servidor
 *
 * Arquitetura de threads:
 *   - Thread principal   : aceita novas conexões TCP em loop
 *   - Thread produtora   : uma por cliente; recebe mensagens e coloca no buffer
 *   - Thread consumidora : única; retira do buffer e faz broadcast para todos
 *
 * Sincronização do buffer compartilhado:
 *   - sem_t semSlots    : conta itens disponíveis para consumo (inicia em 0)
 *   - sem_t semEspacos  : conta espaços livres no buffer    (inicia em TAM_BUFFER)
 *   - pthread_mutex_t   : protege leitura/escrita no buffer circular
 *
 * Protocolo de mensagens (todas as strings enviadas via socket):
 *   bom|<comando>|<conteudo>|eom
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ─── Constantes ────────────────────────────────────────────────────────── */
#define MAX_CLIENTES   32          /* máximo de clientes simultâneos        */
#define TAM_BUFFER     64          /* capacidade do buffer circular          */
#define TAM_MSG       4096         /* tamanho máximo de uma mensagem         */
#define TAM_APELIDO    64          /* tamanho máximo do apelido              */

/* ─── Buffer Compartilhado (Produtor / Consumidor) ──────────────────────── */
char   bufferMsgs[TAM_BUFFER][TAM_MSG]; /* buffer circular de mensagens      */
int    indiceProdutor  = 0;             /* próxima posição para inserção      */
int    indiceConsumidor = 0;            /* próxima posição para retirada      */

sem_t           semSlots;    /* sinaliza itens prontos para consumo          */
sem_t           semEspacos;  /* sinaliza espaços livres para produção        */
pthread_mutex_t mutexBuffer = PTHREAD_MUTEX_INITIALIZER; /* protege o buffer */

/* ─── Tabela de Clientes Conectados ─────────────────────────────────────── */
typedef struct {
    int    fd;                    /* file descriptor do socket do cliente    */
    char   apelido[TAM_APELIDO]; /* apelido escolhido pelo usuário          */
    int    ativo;                 /* 1 = conectado, 0 = desconectado         */
} Cliente;

Cliente        clientes[MAX_CLIENTES];
pthread_mutex_t mutexClientes = PTHREAD_MUTEX_INITIALIZER; /* protege tabela */

/* ─── Protótipos ─────────────────────────────────────────────────────────── */
void  inserirNoBuffer(const char *mensagem);
void *threadConsumidora(void *arg);
void *threadProdutora(void *arg);
void  removerCliente(int fd);
void  parsearComando(const char *pacote, char *comando, char *conteudo);

/* ═══════════════════════════════════════════════════════════════════════════
 * inserirNoBuffer — coloca uma mensagem no buffer circular (lado produtor)
 * Bloqueia se o buffer estiver cheio (sem_wait em semEspacos).
 * ═══════════════════════════════════════════════════════════════════════════ */
void inserirNoBuffer(const char *mensagem) {
    /* Aguarda haver espaço livre no buffer */
    sem_wait(&semEspacos);

    /* Seção crítica: escreve no buffer e avança o índice do produtor */
    pthread_mutex_lock(&mutexBuffer);
    strncpy(bufferMsgs[indiceProdutor], mensagem, TAM_MSG - 1);
    bufferMsgs[indiceProdutor][TAM_MSG - 1] = '\0';
    indiceProdutor = (indiceProdutor + 1) % TAM_BUFFER;
    pthread_mutex_unlock(&mutexBuffer);

    /* Sinaliza que há um novo item disponível para o consumidor */
    sem_post(&semSlots);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * threadConsumidora — única thread que retira mensagens do buffer e
 * faz broadcast para todos os clientes ativos.
 * ═══════════════════════════════════════════════════════════════════════════ */
void *threadConsumidora(void *arg) {
    char mensagem[TAM_MSG];
    char pacote[TAM_MSG + 64];

    while (1) {
        /* Aguarda haver pelo menos um item no buffer */
        sem_wait(&semSlots);

        /* Seção crítica: retira mensagem do buffer */
        pthread_mutex_lock(&mutexBuffer);
        strncpy(mensagem, bufferMsgs[indiceConsumidor], TAM_MSG - 1);
        mensagem[TAM_MSG - 1] = '\0';
        indiceConsumidor = (indiceConsumidor + 1) % TAM_BUFFER;
        pthread_mutex_unlock(&mutexBuffer);

        /* Sinaliza que um espaço ficou livre no buffer */
        sem_post(&semEspacos);

        /* Monta o pacote no protocolo: bom|msg_servidor|<msg>|eom */
        snprintf(pacote, sizeof(pacote), "bom|msg_servidor|%s|eom", mensagem);

        /* Broadcast: envia para todos os clientes ativos */
        pthread_mutex_lock(&mutexClientes);
        for (int i = 0; i < MAX_CLIENTES; i++) {
            if (clientes[i].ativo && clientes[i].fd != -1) {
                send(clientes[i].fd, pacote, strlen(pacote), 0);
            }
        }
        pthread_mutex_unlock(&mutexClientes);

        printf("[Consumidor] Broadcast enviado: %s\n", mensagem);
    }

    pthread_exit(NULL);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * parsearComando — extrai <comando> e <conteudo> de "bom|comando|conteudo|eom"
 * Preenche os buffers passados com strings vazias em caso de erro de formato.
 * ═══════════════════════════════════════════════════════════════════════════ */
void parsearComando(const char *pacote, char *comando, char *conteudo) {
    comando[0]  = '\0';
    conteudo[0] = '\0';

    /* Copia para não modificar o original */
    char copia[TAM_MSG];
    strncpy(copia, pacote, TAM_MSG - 1);
    copia[TAM_MSG - 1] = '\0';

    /* Espera o padrão: bom|<cmd>|<conteudo>|eom */
    char *tok = strtok(copia, "|");
    if (!tok || strcmp(tok, "bom") != 0) return;

    tok = strtok(NULL, "|");
    if (!tok) return;
    strncpy(comando, tok, TAM_APELIDO - 1);

    tok = strtok(NULL, "|");
    if (!tok) return;
    strncpy(conteudo, tok, TAM_MSG - 1);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * removerCliente — marca um cliente como inativo e fecha seu socket.
 * Deve ser chamado com mutexClientes DESBLOQUEADO.
 * ═══════════════════════════════════════════════════════════════════════════ */
void removerCliente(int fd) {
    pthread_mutex_lock(&mutexClientes);
    for (int i = 0; i < MAX_CLIENTES; i++) {
        if (clientes[i].fd == fd) {
            clientes[i].ativo = 0;
            close(clientes[i].fd);
            clientes[i].fd = -1;
            break;
        }
    }
    pthread_mutex_unlock(&mutexClientes);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * threadProdutora — criada para cada cliente conectado.
 * Recebe pacotes do cliente, parseia e coloca mensagens formatadas no buffer.
 * ═══════════════════════════════════════════════════════════════════════════ */
void *threadProdutora(void *arg) {
    int  fd = *((int *) arg);
    free(arg); /* libera o heap alocado em main para passar o fd */

    char pacoteRecebido[TAM_MSG];
    char comando[TAM_APELIDO];
    char conteudo[TAM_MSG];
    char apelido[TAM_APELIDO];
    char msgBuffer[TAM_MSG];

    apelido[0] = '\0';

    printf("[Produtor fd=%d] Thread iniciada.\n", fd);

    while (1) {
        memset(pacoteRecebido, 0, sizeof(pacoteRecebido));

        int bytesRecebidos = recv(fd, pacoteRecebido, sizeof(pacoteRecebido) - 1, 0);
        if (bytesRecebidos <= 0) {
            /* Conexão encerrada de forma inesperada (cliente caiu) */
            if (apelido[0] != '\0') {
                snprintf(msgBuffer, sizeof(msgBuffer),
                         "%s saiu da sala de conversa.", apelido);
                inserirNoBuffer(msgBuffer);
                printf("[Produtor fd=%d] Cliente '%s' desconectou abruptamente.\n",
                       fd, apelido);
            }
            removerCliente(fd);
            pthread_exit(NULL);
        }

        pacoteRecebido[bytesRecebidos] = '\0';
        printf("[Produtor fd=%d] Recebido: %s\n", fd, pacoteRecebido);

        /* Parseia o pacote recebido */
        parsearComando(pacoteRecebido, comando, conteudo);

        if (strcmp(comando, "usuario_entra") == 0) {
            /* Registra o apelido na tabela de clientes */
            strncpy(apelido, conteudo, TAM_APELIDO - 1);
            pthread_mutex_lock(&mutexClientes);
            for (int i = 0; i < MAX_CLIENTES; i++) {
                if (clientes[i].fd == fd) {
                    strncpy(clientes[i].apelido, apelido, TAM_APELIDO - 1);
                    break;
                }
            }
            pthread_mutex_unlock(&mutexClientes);

            snprintf(msgBuffer, sizeof(msgBuffer),
                     "%s entrou na sala de conversa.", apelido);
            inserirNoBuffer(msgBuffer);

        } else if (strcmp(comando, "msg_cliente") == 0) {
            /* Mensagem comum de chat */
            /* Trunca conteudo para garantir que caiba: apelido + " enviou: " + conteudo */
            int espacoConteudo = (int)sizeof(msgBuffer) - (int)strlen(apelido) - 10;
            if (espacoConteudo < 1) espacoConteudo = 1;
            snprintf(msgBuffer, sizeof(msgBuffer),
                     "%s enviou: %.*s", apelido, espacoConteudo, conteudo);
            inserirNoBuffer(msgBuffer);

        } else if (strcmp(comando, "usuario_sai") == 0) {
            /* Desconexão voluntária */
            snprintf(msgBuffer, sizeof(msgBuffer),
                     "%s saiu da sala de conversa.", apelido);
            inserirNoBuffer(msgBuffer);
            printf("[Produtor fd=%d] Cliente '%s' saiu voluntariamente.\n",
                   fd, apelido);
            removerCliente(fd);
            pthread_exit(NULL);
        }
    }

    pthread_exit(NULL);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * main — configura socket, inicia thread consumidora e aceita clientes em loop
 * ═══════════════════════════════════════════════════════════════════════════ */
int main(void) {
    int porta;
    printf("=== Servidor de Chat ===\n");
    printf("Informe a porta TCP para escutar: ");
    if (scanf("%d", &porta) != 1 || porta <= 0 || porta > 65535) {
        fprintf(stderr, "Porta inválida.\n");
        return EXIT_FAILURE;
    }

    /* ── Inicializa tabela de clientes ──────────────────────────────────── */
    for (int i = 0; i < MAX_CLIENTES; i++) {
        clientes[i].fd    = -1;
        clientes[i].ativo = 0;
        memset(clientes[i].apelido, 0, TAM_APELIDO);
    }

    /* ── Inicializa semáforos ────────────────────────────────────────────── */
    /* semSlots   começa em 0  (buffer vazio, consumidor bloqueado)           */
    /* semEspacos começa em TAM_BUFFER (buffer livre, produtor pode inserir)  */
    sem_init(&semSlots,   0, 0);
    sem_init(&semEspacos, 0, TAM_BUFFER);

    /* ── Inicia thread consumidora ──────────────────────────────────────── */
    pthread_t tidConsumidor;
    if (pthread_create(&tidConsumidor, NULL, threadConsumidora, NULL) != 0) {
        perror("Erro ao criar thread consumidora");
        return EXIT_FAILURE;
    }
    /* Thread consumidora roda para sempre; não precisamos de join aqui */
    pthread_detach(tidConsumidor);

    /* ── Cria socket do servidor ────────────────────────────────────────── */
    int serverfd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverfd == -1) {
        perror("Erro ao criar socket do servidor");
        return EXIT_FAILURE;
    }

    /* Reutiliza porta rapidamente após reinicialização */
    int yes = 1;
    setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

    struct sockaddr_in server;
    memset(&server, 0, sizeof(server));
    server.sin_family      = AF_INET;
    server.sin_port        = htons(porta);
    server.sin_addr.s_addr = INADDR_ANY;

    if (bind(serverfd, (struct sockaddr *) &server, sizeof(server)) == -1) {
        perror("Erro no bind");
        return EXIT_FAILURE;
    }

    if (listen(serverfd, MAX_CLIENTES) == -1) {
        perror("Erro no listen");
        return EXIT_FAILURE;
    }

    printf("Servidor escutando na porta %d...\n\n", porta);

    /* ── Loop principal: aceita clientes e cria thread produtora ────────── */
    while (1) {
        struct sockaddr_in clientAddr;
        socklen_t clientLen = sizeof(clientAddr);

        int clientfd = accept(serverfd,
                              (struct sockaddr *) &clientAddr, &clientLen);
        if (clientfd == -1) {
            perror("Erro no accept");
            continue;
        }

        printf("[Main] Novo cliente conectado. fd=%d  IP=%s\n",
               clientfd, inet_ntoa(clientAddr.sin_addr));

        /* Registra o cliente na tabela (slot livre) */
        int slotLivre = -1;
        pthread_mutex_lock(&mutexClientes);
        for (int i = 0; i < MAX_CLIENTES; i++) {
            if (!clientes[i].ativo) {
                clientes[i].fd    = clientfd;
                clientes[i].ativo = 1;
                memset(clientes[i].apelido, 0, TAM_APELIDO);
                slotLivre = i;
                break;
            }
        }
        pthread_mutex_unlock(&mutexClientes);

        if (slotLivre == -1) {
            /* Sem espaço na tabela; recusa a conexão */
            const char *rejMsg = "bom|msg_servidor|Servidor cheio. Tente mais tarde.|eom";
            send(clientfd, rejMsg, strlen(rejMsg), 0);
            close(clientfd);
            printf("[Main] Conexão recusada: servidor cheio.\n");
            continue;
        }

        /* Envia mensagem de boas-vindas conforme protocolo */
        const char *boasVindas = "bom|msg_servidor|Olá! Seja bem-vindo!|eom";
        send(clientfd, boasVindas, strlen(boasVindas), 0);

        /* Aloca fd no heap para passar à thread sem problema de escopo */
        int *fdHeap = (int *) malloc(sizeof(int));
        *fdHeap = clientfd;

        /* Cria thread produtora dedicada a este cliente */
        pthread_t tidProdutor;
        if (pthread_create(&tidProdutor, NULL, threadProdutora, fdHeap) != 0) {
            perror("Erro ao criar thread produtora");
            free(fdHeap);
            removerCliente(clientfd);
            continue;
        }
        /* Thread produtora gerencia seu próprio ciclo de vida */
        pthread_detach(tidProdutor);
    }

    /* Limpeza (inalcançável neste modelo, mas boa prática) */
    sem_destroy(&semSlots);
    sem_destroy(&semEspacos);
    close(serverfd);

    return EXIT_SUCCESS;
}
