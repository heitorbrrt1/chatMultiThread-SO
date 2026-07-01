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

#define MAX_CLIENTES   32
#define TAM_BUFFER     64
#define TAM_MSG       4096
#define TAM_APELIDO    64

char   bufferMsgs[TAM_BUFFER][TAM_MSG];
int    indiceProdutor  = 0;
int    indiceConsumidor = 0;

sem_t           semSlots;
sem_t           semEspacos;
pthread_mutex_t mutexBuffer = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int    fd;
    char   apelido[TAM_APELIDO];
    int    ativo;
} Cliente;

Cliente        clientes[MAX_CLIENTES];
pthread_mutex_t mutexClientes = PTHREAD_MUTEX_INITIALIZER;

void  inserirNoBuffer(const char *mensagem);
void *threadConsumidora(void *arg);
void *threadProdutora(void *arg);
void  removerCliente(int fd);
void  parsearComando(const char *pacote, char *comando, char *conteudo);

void inserirNoBuffer(const char *mensagem) {
    sem_wait(&semEspacos);

    pthread_mutex_lock(&mutexBuffer);
    strncpy(bufferMsgs[indiceProdutor], mensagem, TAM_MSG - 1);
    bufferMsgs[indiceProdutor][TAM_MSG - 1] = '\0';
    indiceProdutor = (indiceProdutor + 1) % TAM_BUFFER;
    pthread_mutex_unlock(&mutexBuffer);

    sem_post(&semSlots);
}

void *threadConsumidora(void *arg) {
    char mensagem[TAM_MSG];
    char pacote[TAM_MSG + 64];

    while (1) {
        sem_wait(&semSlots);

        pthread_mutex_lock(&mutexBuffer);
        strncpy(mensagem, bufferMsgs[indiceConsumidor], TAM_MSG - 1);
        mensagem[TAM_MSG - 1] = '\0';
        indiceConsumidor = (indiceConsumidor + 1) % TAM_BUFFER;
        pthread_mutex_unlock(&mutexBuffer);

        sem_post(&semEspacos);

        snprintf(pacote, sizeof(pacote), "bom|msg_servidor|%s|eom", mensagem);

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

void parsearComando(const char *pacote, char *comando, char *conteudo) {
    comando[0]  = '\0';
    conteudo[0] = '\0';

    char copia[TAM_MSG];
    strncpy(copia, pacote, TAM_MSG - 1);
    copia[TAM_MSG - 1] = '\0';

    char *tok = strtok(copia, "|");
    if (!tok || strcmp(tok, "bom") != 0) return;

    tok = strtok(NULL, "|");
    if (!tok) return;
    strncpy(comando, tok, TAM_APELIDO - 1);

    tok = strtok(NULL, "|");
    if (!tok) return;
    strncpy(conteudo, tok, TAM_MSG - 1);
}

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

void *threadProdutora(void *arg) {
    int  fd = *((int *) arg);
    free(arg);

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

        parsearComando(pacoteRecebido, comando, conteudo);

        if (strcmp(comando, "usuario_entra") == 0) {
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
            int espacoConteudo = (int)sizeof(msgBuffer) - (int)strlen(apelido) - 10;
            if (espacoConteudo < 1) espacoConteudo = 1;
            snprintf(msgBuffer, sizeof(msgBuffer),
                     "%s enviou: %.*s", apelido, espacoConteudo, conteudo);
            inserirNoBuffer(msgBuffer);

        } else if (strcmp(comando, "usuario_sai") == 0) {
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

int main(void) {
    int porta;
    printf("=== Servidor de Chat ===\n");
    printf("Informe a porta TCP para escutar: ");
    if (scanf("%d", &porta) != 1 || porta <= 0 || porta > 65535) {
        fprintf(stderr, "Porta inválida.\n");
        return EXIT_FAILURE;
    }

    for (int i = 0; i < MAX_CLIENTES; i++) {
        clientes[i].fd    = -1;
        clientes[i].ativo = 0;
        memset(clientes[i].apelido, 0, TAM_APELIDO);
    }

    sem_init(&semSlots,   0, 0);
    sem_init(&semEspacos, 0, TAM_BUFFER);

    pthread_t tidConsumidor;
    if (pthread_create(&tidConsumidor, NULL, threadConsumidora, NULL) != 0) {
        perror("Erro ao criar thread consumidora");
        return EXIT_FAILURE;
    }
    pthread_detach(tidConsumidor);

    int serverfd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverfd == -1) {
        perror("Erro ao criar socket do servidor");
        return EXIT_FAILURE;
    }

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
            const char *rejMsg = "bom|msg_servidor|Servidor cheio. Tente mais tarde.|eom";
            send(clientfd, rejMsg, strlen(rejMsg), 0);
            close(clientfd);
            printf("[Main] Conexão recusada: servidor cheio.\n");
            continue;
        }

        const char *boasVindas = "bom|msg_servidor|Olá! Seja bem-vindo!|eom";
        send(clientfd, boasVindas, strlen(boasVindas), 0);

        int *fdHeap = (int *) malloc(sizeof(int));
        *fdHeap = clientfd;

        pthread_t tidProdutor;
        if (pthread_create(&tidProdutor, NULL, threadProdutora, fdHeap) != 0) {
            perror("Erro ao criar thread produtora");
            free(fdHeap);
            removerCliente(clientfd);
            continue;
        }
        pthread_detach(tidProdutor);
    }

    sem_destroy(&semSlots);
    sem_destroy(&semEspacos);
    close(serverfd);

    return EXIT_SUCCESS;
}
