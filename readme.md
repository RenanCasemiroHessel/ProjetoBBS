# ProjetoBBS

Sistema de troca de mensagens instantâneas inspirado no modelo IRC/BBS,
desenvolvido para a disciplina de Sistemas Distribuídos.

## Descrição

O ProjetoBBS implementa um sistema distribuído de mensagens onde bots (clientes)
podem realizar login, criar canais, publicar mensagens e receber mensagens de
outros bots em tempo real. A comunicação é intermediada por um broker central
para operações REQ/REP e por um proxy dedicado para o padrão PUB/SUB.

## Arquitetura

```
client1 (Java) --REQ--> broker.py <--> server1.py
client2 (Java) --REQ--> <--> server2.py
|
PUB
|
pubsub_proxy.py
XSUB=5557 / XPUB=5558
|
SUB
client1 Subscriber (Java)
client2 Subscriber (Java)
```

O broker recebe as requisições dos clientes e as distribui entre os servidores
disponíveis usando o padrão ROUTER/DEALER do ZeroMQ. O proxy PUB/SUB é mantido
separado do broker para isolar as responsabilidades: o broker gerencia o ciclo
REQ/REP (login, criação e listagem de canais, publicação), enquanto o proxy
gerencia a distribuição das mensagens publicadas para todos os subscribers.

## Tecnologias e Escolhas

### Linguagens
- **Python 3.13** — utilizado no broker, servidores e proxy PUB/SUB
- **Java 21** — utilizado nos clientes (bots)

A combinação foi escolhida pela simplicidade do Python para gerenciar
a lógica do servidor e pela preferência ao Java para estruturar
as mensagens enviadas pelo cliente.

### Tecnologias

- ZeroMQ (jeromq) | Comunicação assíncrona entre processos 
- MessagePack | Serialização binária de mensagens 
- Python 3.13 | Servidor, broker e proxy 
- Java 21 + Maven | Cliente 
- Docker / Docker Compose | Orquestração dos containers 

### Serialização
- **MessagePack** — serialização binária de todas as mensagens trocadas na rede
  - Escolhido por ser mais simples que ProtoBuf (sem necessidade de schemas)
  - Suporte nativo em Python (`msgpack`) e Java (`jackson-dataformat-msgpack`)
  - Todas as mensagens incluem campo `timestamp` obrigatório (Unix epoch)
  - As mensagens publicadas pelos bots também são serializadas em MessagePack
    antes de serem enviadas pelo proxy, garantindo consistência no protocolo

### Troca de Mensagens PUB/SUB
- **Padrão XSUB/XPUB via proxy dedicado** — escolhido para desacoplar
  publishers e subscribers sem que precisem se conhecer diretamente
  - O servidor publica no proxy via socket PUB (porta 5557)
  - O proxy redistribui para todos os clientes inscritos via socket SUB (porta 5558)
  - O proxy foi separado do broker para que cada componente tenha
    uma única responsabilidade: o broker gerencia REQ/REP, o proxy gerencia PUB/SUB
  - O filtro de inscrição é feito por nome de canal, permitindo que cada bot
    receba apenas as mensagens dos canais em que está inscrito

### Comportamento dos Bots
Cada bot segue um loop contínuo com as seguintes regras:
1. Se existem menos de 5 canais disponíveis → cria um novo canal
2. Se está inscrito em menos de 3 canais → se inscreve em mais um
3. Escolhe um canal aleatório e publica 10 mensagens com intervalo de 1 segundo

### Persistência
- **JSON** — armazenamento interno de cada servidor em `/data/server_<ID>.json`
  - Cada servidor mantém seu próprio arquivo, não compartilhado entre instâncias
  - Armazena: logins realizados (com timestamp), canais criados e **mensagens publicadas**
  - As mensagens publicadas são salvas com canal, conteúdo, remetente e timestamp,
    permitindo auditoria completa do histórico de publicações de cada servidor
  - Quando um canal criado em outro servidor chega via publish (round-robin do broker),
    o servidor o replica automaticamente em seu estado local antes de persistir

## Estrutura do Projeto

```
ProjetoBBS/
├── broker/
│ ├── Dockerfile
│ └── broker.py
├── proxy/
│ ├── Dockerfile
│ └── pubsub_proxy.py
├── server/
│ ├── Dockerfile
│ └── server.py
├── client/
│ ├── Dockerfile
│ ├── pom.xml
│ └── src/main/java/com/projetobbs/
│ ├── Client.java
│ ├── Publisher.java
│ └── Subscriber.java
├── docker-compose.yml
└── README.md
```