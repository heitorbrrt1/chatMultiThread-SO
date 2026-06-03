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

/* ─── Constantes ────────────────────────────────────────────────────────── */
#define TAM_MSG     4096
#define TAM_APELIDO   64

/* ─── Variável global: file descriptor do socket ────────────────────────── */
/* Compartilhada entre a thread principal e a thread auxiliar.               */
/* Apenas leitura após a conexão ser estabelecida → sem necessidade de mutex */
int sockfd_global = -1;

/* ─── Flag de desconexão ─────────────────────────────────────────────────── */
/* Quando 1, a thread auxiliar deve encerrar seu loop.                       */
volatile int desconectado = 0;

/* ═══════════════════════════════════════════════════════════════════════════
 * parsearConteudo — extrai o campo <conteudo> de "bom|comando|conteudo|eom"
 * Escreve string vazia em 'saida' caso o formato seja inválido.
 * ═══════════════════════════════════════════════════════════════════════════ */
void parsearConteudo(const char *pacote, char *saida, int tamSaida) {
    saida[0] = '\0';

    char copia[TAM_MSG];
    strncpy(copia, pacote, TAM_MSG - 1);
    copia[TAM_MSG - 1] = '\0';

    /* Espera o padrão: bom|<cmd>|<conteudo>|eom */
    char *tok = strtok(copia, "|");
    if (!tok || strcmp(tok, "bom") != 0) return;

    tok = strtok(NULL, "|"); /* comando */
    if (!tok) return;

    tok = strtok(NULL, "|"); /* conteúdo */
    if (!tok) return;

    strncpy(saida, tok, tamSaida - 1);
    saida[tamSaida - 1] = '\0';
}

/* ═══════════════════════════════════════════════════════════════════════════
 * threadAuxiliar — fica em loop de recv(), recebe pacotes do servidor,
 * extrai a mensagem e imprime na tela do usuário.
 * Encerra quando a conexão é fechada ou desconectado == 1.
 * ═══════════════════════════════════════════════════════════════════════════ */
void *threadAuxiliar(void *arg) {
    char pacote[TAM_MSG];
    char conteudo[TAM_MSG];

    while (!desconectado) {
        memset(pacote, 0, sizeof(pacote));

        int bytesRecebidos = recv(sockfd_global, pacote, sizeof(pacote) - 1, 0);
        if (bytesRecebidos <= 0) {
            /* Servidor fechou a conexão */
            if (!desconectado) {
                printf("\n[Sistema] Conexão com o servidor encerrada.\n");
                desconectado = 1;
            }
            break;
        }

        pacote[bytesRecebidos] = '\0';

        /* Extrai e imprime apenas o conteúdo da mensagem */
        parsearConteudo(pacote, conteudo, sizeof(conteudo));
        if (conteudo[0] != '\0') {
            printf("\n[Chat] %s\n", conteudo);
            /* Reimprime o prompt para não confundir o usuário */
            printf("Digite sua mensagem (ou 'tchau' para sair): ");
            fflush(stdout);
        }
    }

    pthread_exit(NULL);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * main — solicita dados de conexão, conecta ao servidor, inicia thread
 * auxiliar e entra no loop de leitura do teclado.
 * ═══════════════════════════════════════════════════════════════════════════ */
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

    /* Consome o '\n' restante no buffer do stdin antes de usar fgets */
    getchar();

    /* ── Cria o socket TCP ──────────────────────────────────────────────── */
    sockfd_global = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd_global == -1) {
        perror("Erro ao criar socket");
        return EXIT_FAILURE;
    }

    /* ── Configura endereço do servidor ─────────────────────────────────── */
    struct sockaddr_in servidor;
    memset(&servidor, 0, sizeof(servidor));
    servidor.sin_family      = AF_INET;
    servidor.sin_port        = htons(porta);
    servidor.sin_addr.s_addr = inet_addr(enderecoIP);

    /* ── Conecta ao servidor ─────────────────────────────────────────────── */
    if (connect(sockfd_global, (struct sockaddr *) &servidor,
                sizeof(servidor)) == -1) {
        perror("Erro ao conectar ao servidor");
        return EXIT_FAILURE;
    }
    printf("\nConectado ao servidor %s:%d\n\n", enderecoIP, porta);

    /* ── Recebe e exibe a mensagem de boas-vindas do servidor ───────────── */
    char pacote[TAM_MSG];
    char conteudo[TAM_MSG];
    int  bytesRecebidos = recv(sockfd_global, pacote, sizeof(pacote) - 1, 0);
    if (bytesRecebidos > 0) {
        pacote[bytesRecebidos] = '\0';
        parsearConteudo(pacote, conteudo, sizeof(conteudo));
        printf("[Servidor] %s\n\n", conteudo);
    }

    /* ── Envia comando de entrada na sala ───────────────────────────────── */
    char cmdEntrada[TAM_MSG];
    snprintf(cmdEntrada, sizeof(cmdEntrada),
             "bom|usuario_entra|%s|eom", apelido);
    send(sockfd_global, cmdEntrada, strlen(cmdEntrada), 0);

    /* ── Inicia thread auxiliar (recebe mensagens do servidor) ──────────── */
    pthread_t tidAuxiliar;
    if (pthread_create(&tidAuxiliar, NULL, threadAuxiliar, NULL) != 0) {
        perror("Erro ao criar thread auxiliar");
        close(sockfd_global);
        return EXIT_FAILURE;
    }

    /* ── Loop da thread principal: lê teclado e envia ao servidor ───────── */
    char entrada[TAM_MSG];
    char cmdSaida[TAM_MSG];

    while (!desconectado) {
        printf("Digite sua mensagem (ou 'tchau' para sair): ");
        fflush(stdout);

        if (fgets(entrada, sizeof(entrada), stdin) == NULL) {
            /* EOF (Ctrl+D) — trata como saída */
            break;
        }

        /* Remove o '\n' do fgets */
        size_t len = strlen(entrada);
        if (len > 0 && entrada[len - 1] == '\n') {
            entrada[len - 1] = '\0';
            len--;
        }

        /* Ignora linhas vazias */
        if (len == 0) continue;

        if (strcmp(entrada, "tchau") == 0) {
            /* Envia comando de saída e encerra */
            snprintf(cmdSaida, sizeof(cmdSaida),
                     "bom|usuario_sai|%s|eom", apelido);
            send(sockfd_global, cmdSaida, strlen(cmdSaida), 0);
            desconectado = 1;
            break;
        }

        /* Monta o pacote: "bom|msg_cliente|<msg>|eom" — limita tamanho da msg */
        int espacoMsg = (int)sizeof(cmdSaida) - 21; /* 21 = len("bom|msg_cliente||eom") */
        snprintf(cmdSaida, sizeof(cmdSaida),
                 "bom|msg_cliente|%.*s|eom", espacoMsg, entrada);
        send(sockfd_global, cmdSaida, strlen(cmdSaida), 0);
    }

    /* ── Encerramento ────────────────────────────────────────────────────── */
    desconectado = 1;          /* garante que a thread auxiliar também pare  */
    shutdown(sockfd_global, SHUT_RDWR); /* desbloqueia recv() na thread aux   */
    pthread_join(tidAuxiliar, NULL);    /* aguarda thread auxiliar terminar   */
    close(sockfd_global);

    printf("\n[Sistema] Conexão encerrada. Até logo!\n\n");

    return EXIT_SUCCESS;
}
