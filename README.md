# Smart LLM Gateway

[English](#english) · [Português (Brasil)](#português-brasil)

## English

Smart LLM Gateway is an open-source, Rust-based gateway for applications that
need a stable, OpenAI-compatible interface while routing requests to one or
more compatible upstream providers.

It separates the model name used by a client from the upstream account and
model that fulfill a request. This lets operators evolve routes and fallbacks
without requiring client-side configuration changes.

### Current capabilities

- OpenAI-compatible `POST /v1/chat/completions` endpoint.
- Stable logical model names, prioritized routes, and model fallbacks.
- Gateway-key authentication.
- Generic OpenAI-compatible upstream transport.
- SQLite for a self-contained local installation and PostgreSQL for shared
  deployments.
- Failure classification, circuit-state persistence, and attempt recording.
- Streaming and non-streaming chat completions. Streaming responses are
  forwarded as SSE without buffering the full completion, and fallback stops
  once an upstream streaming response commits.

The project is intentionally provider-neutral: PostgreSQL hosting choices,
including managed hosts, are deployment decisions rather than product
dependencies.

### Quick start

Prerequisites: the Rust toolchain specified in
[`rust-toolchain.toml`](rust-toolchain.toml).

```sh
cargo run -p smart-llm-gateway -- init --database gateway.sqlite
cargo run -p smart-llm-gateway -- key create --database gateway.sqlite
cargo run -p smart-llm-gateway -- model create --database gateway.sqlite my-logical-model
cargo run -p smart-llm-gateway -- account create --database gateway.sqlite openrouter --credential-env env:OPENROUTER_API_KEY --base-url https://openrouter.ai/api
cargo run -p smart-llm-gateway -- route add --database gateway.sqlite primary --model my-logical-model --account openrouter --upstream-model deepseek/deepseek-chat-v3-0324
cargo run -p smart-llm-gateway -- --help
```

Complete the local account and route configuration through the CLI before
starting the server. Provider credentials must be supplied privately at runtime
and must never be committed to a repository, copied into issue text, or sent by
clients to the gateway.

Start the gateway after configuration:

```sh
cargo run -p smart-llm-gateway -- serve --database gateway.sqlite
```

Use a PostgreSQL connection URL in place of the SQLite file path for a shared
deployment. See the command help for the available administrative commands and
options.

### API

The initial public API is compatible with OpenAI chat-completion clients:

```text
POST /v1/chat/completions
GET  /healthz
```

Authenticate requests with a gateway key using the standard Bearer
authorization scheme. The key is shown only when it is created; store it in an
appropriate secret manager.

### Development

Run the local quality gate before proposing a change:

```sh
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Product scope and architectural invariants are documented in
[`PRODUCT.md`](PRODUCT.md) and [`ARCHITECTURE.md`](ARCHITECTURE.md). Review
[`SECURITY.md`](SECURITY.md), [`SUPPORT.md`](SUPPORT.md),
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md), and the
[release process](.github/RELEASE_PROCESS.md) before participating in the
project.

### Status and roadmap

The initial OpenAI-compatible slice, including streaming, is implemented.
Additional inbound protocols, provider-native quota synchronization, billing
reconciliation, and distribution automation remain roadmap work.

### License

Licensed under the [Apache License, Version 2.0](LICENSE).

## Português (Brasil)

O Smart LLM Gateway é um gateway open source, implementado em Rust, para
aplicações que precisam de uma interface estável compatível com OpenAI enquanto
roteiam requisições para um ou mais provedores compatíveis.

Ele separa o nome lógico do modelo usado pelo cliente da conta e do modelo
upstream que atendem a requisição. Assim, é possível evoluir rotas e fallbacks
sem alterar a configuração dos clientes.

### Recursos atuais

- Endpoint `POST /v1/chat/completions` compatível com OpenAI.
- Nomes lógicos estáveis, rotas priorizadas e fallback entre modelos.
- Autenticação por chave do gateway.
- Transporte upstream genérico compatível com OpenAI.
- SQLite para instalação local autocontida e PostgreSQL para implantações
  compartilhadas.
- Classificação de falhas, persistência do estado de circuitos e registro de
  tentativas.
- Chat completions com e sem streaming. Respostas em streaming são encaminhadas
  como SSE sem armazenar a conclusão inteira em memória, e o fallback termina
  quando a resposta em streaming do upstream é comprometida.

O projeto é neutro em relação a provedores: a escolha de hospedagem PostgreSQL,
inclusive serviços gerenciados, é uma decisão de implantação e não uma
dependência do produto.

### Início rápido

Pré-requisito: use a ferramenta Rust indicada em
[`rust-toolchain.toml`](rust-toolchain.toml).

```sh
cargo run -p smart-llm-gateway -- init --database gateway.sqlite
cargo run -p smart-llm-gateway -- key create --database gateway.sqlite
cargo run -p smart-llm-gateway -- model create --database gateway.sqlite meu-modelo-logico
cargo run -p smart-llm-gateway -- account create --database gateway.sqlite openrouter --credential-env env:OPENROUTER_API_KEY --base-url https://openrouter.ai/api
cargo run -p smart-llm-gateway -- route add --database gateway.sqlite primary --model meu-modelo-logico --account openrouter --upstream-model deepseek/deepseek-chat-v3-0324
cargo run -p smart-llm-gateway -- --help
```

Conclua localmente a configuração de contas e rotas pela CLI antes de iniciar o
servidor. Credenciais de provedores devem ser fornecidas de forma privada em
tempo de execução; nunca as versione, copie para issues ou envie pelos clientes
ao gateway.

Após a configuração, inicie o gateway:

```sh
cargo run -p smart-llm-gateway -- serve --database gateway.sqlite
```

Para uma implantação compartilhada, use uma URL de conexão PostgreSQL no lugar
do caminho do arquivo SQLite. Consulte a ajuda dos comandos para as opções
administrativas disponíveis.

### API

A API pública inicial é compatível com clientes de chat completion da OpenAI:

```text
POST /v1/chat/completions
GET  /healthz
```

Autentique as chamadas com uma chave do gateway pelo esquema padrão de
autorização Bearer. A chave é mostrada somente na criação; guarde-a em um
gerenciador de segredos apropriado.

### Desenvolvimento

Execute o gate local antes de propor uma mudança:

```sh
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

O escopo do produto e as invariantes arquiteturais estão em
[`PRODUCT.md`](PRODUCT.md) e [`ARCHITECTURE.md`](ARCHITECTURE.md). Consulte
[`SECURITY.md`](SECURITY.md), [`SUPPORT.md`](SUPPORT.md),
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) e o
[processo de release](.github/RELEASE_PROCESS.md) antes de participar do
projeto.

### Status e roadmap

A fatia inicial compatível com OpenAI, incluindo streaming, está implementada.
Protocolos de entrada adicionais, sincronização de cotas nativas dos provedores,
reconciliação de cobrança e automação de distribuição continuam no roadmap.

### Licença

Licenciado sob a [Apache License, Version 2.0](LICENSE).
