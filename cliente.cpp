/*
 * cliente.cpp — Cliente de Chat
 *
 * Disciplina: Sistemas Operacionais — IFG Campus Anápolis
 * Compilar e executar:
 *   g++ -pthread cliente.cpp -o cliente && ./cliente
 *
 * Arquitetura de threads:
 *   - Thread principal  : lê mensagens do teclado e envia ao servidor
 *   - Thread auxiliar   : recebe mensagens do servidor e imprime na tela
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

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define TAM_MSG     4096
#define TAM_APELIDO   64

int sockfd_global = -1;

volatile int desconectado = 0;

void parsearConteudo(const char *pacote, char *saida, int tamSaida) {
    saida[0] = '\0';

    char copia[TAM_MSG];
    strncpy(copia, pacote, TAM_MSG - 1);
    copia[TAM_MSG - 1] = '\0';

    char *tok = strtok(copia, "|");
    if (!tok || strcmp(tok, "bom") != 0) return;

    tok = strtok(NULL, "|");
    if (!tok) return;

    tok = strtok(NULL, "|");
    if (!tok) return;

    strncpy(saida, tok, tamSaida - 1);
    saida[tamSaida - 1] = '\0';
}

void *threadAuxiliar(void *arg) {
    char pacote[TAM_MSG];
    char conteudo[TAM_MSG];

    while (!desconectado) {
        memset(pacote, 0, sizeof(pacote));

        int bytesRecebidos = recv(sockfd_global, pacote, sizeof(pacote) - 1, 0);
        if (bytesRecebidos <= 0) {
            if (!desconectado) {
                printf("\n[Sistema] Conexão com o servidor encerrada.\n");
                desconectado = 1;
            }
            break;
        }

        pacote[bytesRecebidos] = '\0';

        parsearConteudo(pacote, conteudo, sizeof(conteudo));
        if (conteudo[0] != '\0') {
            printf("\n[Chat] %s\n", conteudo);
            printf("Digite sua mensagem (ou 'tchau' para sair): ");
            fflush(stdout);
        }
    }

    pthread_exit(NULL);
}

int main(void) {
    char enderecoIP[64];
    int  porta;
    char apelido[TAM_APELIDO];

    printf("=== Cliente de Chat ===\n");
    printf("Endereço IP do servidor: ");
    scanf("%63s", enderecoIP);
    printf("Porta do servidor: ");
    scanf("%d", &porta);
    printf("Apelido desejado: ");
    scanf("%63s", apelido);

    getchar();

    sockfd_global = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd_global == -1) {
        perror("Erro ao criar socket");
        return EXIT_FAILURE;
    }

    struct sockaddr_in servidor;
    memset(&servidor, 0, sizeof(servidor));
    servidor.sin_family      = AF_INET;
    servidor.sin_port        = htons(porta);
    servidor.sin_addr.s_addr = inet_addr(enderecoIP);

    if (connect(sockfd_global, (struct sockaddr *) &servidor,
                sizeof(servidor)) == -1) {
        perror("Erro ao conectar ao servidor");
        return EXIT_FAILURE;
    }
    printf("\nConectado ao servidor %s:%d\n\n", enderecoIP, porta);

    char pacote[TAM_MSG];
    char conteudo[TAM_MSG];
    int  bytesRecebidos = recv(sockfd_global, pacote, sizeof(pacote) - 1, 0);
    if (bytesRecebidos > 0) {
        pacote[bytesRecebidos] = '\0';
        parsearConteudo(pacote, conteudo, sizeof(conteudo));
        printf("[Servidor] %s\n\n", conteudo);
    }

    char cmdEntrada[TAM_MSG];
    snprintf(cmdEntrada, sizeof(cmdEntrada),
             "bom|usuario_entra|%s|eom", apelido);
    send(sockfd_global, cmdEntrada, strlen(cmdEntrada), 0);

    pthread_t tidAuxiliar;
    if (pthread_create(&tidAuxiliar, NULL, threadAuxiliar, NULL) != 0) {
        perror("Erro ao criar thread auxiliar");
        close(sockfd_global);
        return EXIT_FAILURE;
    }

    char entrada[TAM_MSG];
    char cmdSaida[TAM_MSG];

    while (!desconectado) {
        printf("Digite sua mensagem (ou 'tchau' para sair): ");
        fflush(stdout);

        if (fgets(entrada, sizeof(entrada), stdin) == NULL) {
            break;
        }

        size_t len = strlen(entrada);
        if (len > 0 && entrada[len - 1] == '\n') {
            entrada[len - 1] = '\0';
            len--;
        }

        if (len == 0) continue;

        if (strcmp(entrada, "tchau") == 0) {
            snprintf(cmdSaida, sizeof(cmdSaida),
                     "bom|usuario_sai|%s|eom", apelido);
            send(sockfd_global, cmdSaida, strlen(cmdSaida), 0);
            desconectado = 1;
            break;
        }

        int espacoMsg = (int)sizeof(cmdSaida) - 21;
        snprintf(cmdSaida, sizeof(cmdSaida),
                 "bom|msg_cliente|%.*s|eom", espacoMsg, entrada);
        send(sockfd_global, cmdSaida, strlen(cmdSaida), 0);
    }

    desconectado = 1;
    shutdown(sockfd_global, SHUT_RDWR);
    pthread_join(tidAuxiliar, NULL);
    close(sockfd_global);

    printf("\n[Sistema] Conexão encerrada. Até logo!\n\n");

    return EXIT_SUCCESS;
}
