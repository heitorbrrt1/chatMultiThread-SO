Alunos: Heitor Barreto e Ricardo Jefferson

# Chat Multicliente Multithread (Sistemas Operacionais)

Este projeto consiste em uma aplicação de chat distribuída baseada na arquitetura **Cliente-Servidor**, implementada em C++ utilizando a API de sockets TCP do Linux/UNIX, threads POSIX (`pthread`), exclusão mútua (`pthread_mutex_t`) e semáforos (`sem_t`).

O projeto foi desenvolvido para atender aos requisitos práticos da disciplina de **Sistemas Operacionais** do Curso de Bacharelado em Ciência da Computação (IFG Campus Anápolis).

---

## 🚀 Arquitetura do Sistema

A aplicação baseia-se no modelo clássico de sincronização **Produtor-Consumidor** usando um buffer circular compartilhado.

### Principais Componentes:
* **Servidor (Thread Principal)**: Escuta conexões TCP na porta especificada e, a cada nova conexão bem-sucedida, instancia uma `threadProdutora` dedicada para receber mensagens daquele cliente específico.
* **Thread Produtora**: Recebe os pacotes do cliente correspondente, valida o protocolo e insere as mensagens formatadas no buffer circular global de maneira segura.
* **Thread Consumidora**: Responsável por esvaziar o buffer circular global e efetuar o *broadcast* (retransmissão) de cada mensagem a todos os clientes conectados.
* **Cliente (Thread Principal)**: Lê as mensagens do teclado (`stdin`), encapsula-as no protocolo do chat e as envia para o servidor via socket TCP.
* **Cliente (Thread Auxiliar)**: Executa em segundo plano monitorando o socket de rede para receber e exibir imediatamente no terminal as mensagens retransmitidas pelo servidor.

---

## 🛠️ Como Compilar

A compilação do projeto requer suporte às threads POSIX. Utilize os comandos abaixo no terminal Linux (GCC):

```bash
# Compilar o Servidor
g++ -pthread servidor.cpp -o servidor

# Compilar o Cliente
g++ -pthread cliente.cpp -o cliente
```

---

## 💻 Como Executar

### 1. Inicializar o Servidor
Inicie o executável do servidor em uma máquina ou terminal:
```bash
./servidor
```
O servidor solicitará que você digite a porta TCP em que deseja escutar as conexões (ex: `8080`):
```text
=== Servidor de Chat ===
Informe a porta TCP para escutar: 8080
Servidor escutando na porta 8080...
```

### 2. Conectar os Clientes
Em outra máquina (ou em novos terminais na mesma máquina), execute o cliente:
```bash
./cliente
```
Você deverá preencher os dados de conexão solicitados:
```text
=== Cliente de Chat ===
Endereço IP do servidor: 127.0.0.1
Porta do servidor: 8080
Apelido desejado: Hugo

Conectado ao servidor 127.0.0.1:8080
[Servidor] Olá! Seja bem-vindo!

Digite sua mensagem (ou 'tchau' para sair): 
```

Qualquer mensagem digitada por um cliente será retransmitida pelo servidor para todos os demais participantes da sala.

---

## 💬 Protocolo de Comunicação

O protocolo de troca de dados utiliza delimitadores rígidos para garantir a integridade dos pacotes e evitar aglutinações ou fragmentações no transporte TCP:

$$\text{bom} \mid \text{comando} \mid \text{conteúdo} \mid \text{eom}$$

* **`bom`** (Beginning of Message): Cabeçalho padrão de início.
* **`comando`**: Define a ação (`usuario_entra`, `usuario_sai`, `msg_cliente`, `msg_servidor`).
* **`conteúdo`**: A informação textual transmitida.
* **`eom`** (End of Message): Rodapé padrão de fim.

### Exemplo de Fluxo:
* **Login**: `bom|usuario_entra|Hugo|eom`
* **Mensagem**: `bom|msg_cliente|Olá pessoal!|eom`
* **Logout/Saída**: `bom|usuario_sai|Hugo|eom` (ou enviando a palavra `"tchau"` no console).

---