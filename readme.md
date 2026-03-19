# ProjetoBBS

Sistema de troca de mensagens instantâneas inspirado no modelo IRC/BBS,
desenvolvido para a disciplina de Sistemas Distribuídos.

## Descrição

O ProjetoBBS implementa um sistema distribuído de mensagens onde bots (clientes)
podem realizar login, criar canais e listar canais disponíveis em servidores.
A comunicação entre todos os serviços é intermediada por um broker central.

## Arquitetura

```
client1 (Java) ─┐ ┌─ server1 (Python)   

           |-- broker --|            

client2 (Java) ─┘ └─ server2 (Python)
```

O broker recebe as requisições dos clientes e as distribui entre os servidores
disponíveis usando o padrão ROUTER/DEALER do ZeroMQ.

## Tecnologias e Escolhas

### Linguagens
- Python 3.13 — utilizado no broker e nos servidores
- Java 21 — utilizado nos clientes (bots)

A combinação foi escolhida pela simplicidade do Python para gerenciar
a lógica do servidor e pela preferência ao Java
para estruturar as mensagens enviadas pelo cliente.

### Comunicação
- **ZeroMQ** — biblioteca de mensageria assíncrona
  - Padrão **ROUTER/DEALER** no broker para balanceamento entre servidores
  - Padrão **REQ/REP** entre clientes e servidores para operações síncronas

### Serialização
- **MessagePack** — serialização binária das mensagens trocadas na rede
  - Escolhido por ser mais simples que ProtoBuf (sem necessidade de schemas)
  - Suporte nativo em Python (`msgpack`) e Java (`jackson-dataformat-msgpack`)
  - Todas as mensagens incluem campo `timestamp` obrigatório (Unix epoch)

### Persistência
- **JSON** — armazenamento interno de cada servidor em `/data/server_<ID>.json`
  - Cada servidor mantém seu próprio arquivo, não compartilhado entre instâncias
  - Armazena logins realizados (com timestamp) e canais criados
  
## Estrutura do Projeto

```
ProjetoBBS/
├── broker/
│   ├── Dockerfile
│   └── broker.py
├── server/
│   ├── Dockerfile
│   └── server.py
├── client/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/projetobbs/Client.java
├── docker-compose.yml
└── README.md
```
