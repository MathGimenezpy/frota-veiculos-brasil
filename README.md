Frota-Veiculos-Brasil
---
Projeto de engenharia de dados para análise da frota de veículos do Brasil a partir de dados do RENAVAM, com pipeline automatizado, histórico por snapshots e decisões semânticas explícitas.

Princípio Central
---
Pipeline gera a verdade → Banco replica → BI consome

Visão Geral da Arquitetura
---
Fluxo completo dos dados:

→ CKAN (RENAVAM)

→ Bronze (ingestão bruta)

→ Silver (tratamento semântico)

→ Gold (métricas analíticas)

→ Parquet (outputs)

→ PostgreSQL / Supabase (camada de serviço)

→ Power BI (consumo analítico)

Arquitetura por Camadas
---
Bronze — Ingestão

-> Consumo de dados via CKAN (RENAVAM)

-> Download de arquivos CSV/ZIP em memória

-> Padronização mínima de colunas (lowercase, underscore)

-> Persistência dos dados brutos em Parquet

Regras:

-> Nenhuma semântica

-> Nenhuma validação de negócio

-> Nenhuma agregação

-> Aceita o dado exatamente como fornecido

Silver — Semântica
---
Responsável por:

-> Limpeza estrutural

-> Padronização textual (uppercase, remoção de acentos)

-> Mapeamento explícito das colunas reais do dataset

-> Aplicação de regras semânticas

-> Validação de schema canônico

-> Enriquecimento leve permitido (ex: idade do veículo)

-> Todas as decisões de negócio vivem nesta camada.


Gold — Analítica
---
Responsável apenas por métricas analíticas, sem qualquer nova inferência semântica.

Métricas geradas:

-> Perfil da frota por UF

-> Concentração de modelos (Pareto)

-> Penetração de montadoras

-> Penetração de modelos

-> Maturidade da frota por município

-> Proxy de emplacamento via comparação de snapshots

Dimensões Semânticas
---
Conceito

As dimensões representam regras de negócio e contratos semânticos, não dados analíticos.

Elas existem para:

-> Padronizar valores textuais inconsistentes (ex: montadoras escritas de diversas formas)

-> Controlar domínios (ex: UF)

-> Garantir reprodutibilidade das métricas

-> Tornar decisões de negócio explícitas e auditáveis

Por esse motivo, as dimensões são versionadas no GitHub.

Uso das Dimensões no Pipeline:

-> Consumidas exclusivamente na camada Silver

-> Utilizadas para validação, classificação e padronização

-> A camada Gold consome apenas dados já tratados

Banco de Dados (Supabase)
---
O PostgreSQL (Supabase) é utilizado exclusivamente como camada de serviço.

Características:

-> Não é fonte de verdade

-> Pode ser recriado a qualquer momento

-> Recebe dados consolidados da camada Gold

-> Serve dados para ferramentas de BI

-> Se o banco for perdido, todo o ambiente pode ser reconstruído a partir do pipeline.

Power BI
---
O Power BI consome os dados a partir do PostgreSQL/Supabase.

Características:

-> Não consome dados diretamente do GitHub

-> Não consome arquivos Parquet diretamente

-> Utiliza o banco como camada intermediária estável

Versionamento (GitHub)
---
O repositório versiona apenas o que define comportamento e regras do pipeline.

Versionado

-> Código do pipeline (Bronze, Silver, Gold, Orchestrator)

-> GitHub Actions

-> README e documentação

-> requirements.txt

-> Dimensões semânticas (data/dimensions/*.parquet)

Não versionado

-> Dados brutos

-> Snapshots históricos

-> Outputs analíticos da camada Gold

-> Arquivos Parquet gerados por execução

-> Ambiente virtual (.venv)

Regra adotada:

-> Dados não são versionados.

-> Regras e contratos semânticos são versionados.

Persistência de dados e execução em CI (GitHub Actions)
---
Este projeto pode ser executado tanto localmente quanto em ambiente automatizado via GitHub Actions. É importante entender como os dados são tratados em cada contexto.

Ferramentas necessárias:

-> Git

-> Python 3.10+ (recomendado 3.11 ou 3.12)

-> pip

-> Windows, Linux ou macOS

Execução local
---
Em execução local, o pipeline gera e mantém os arquivos Parquet nas seguintes pastas:

-> data/bronze/ — dados brutos ingeridos do CKAN

-> data/silver/ — snapshot semântico tratado (base para análises históricas)

-> data/gold/ — métricas analíticas finais

Esses arquivos permanecem no disco local, permitindo:

-> inspeção manual,

-> reexecuções incrementais,

-> comparação entre snapshots (ex.: proxy de emplacamento).

Execução via GitHub Actions (CI)
---
No GitHub Actions, o pipeline roda em um runner temporário. Nesse ambiente:

-> Os arquivos Parquet gerados em data/bronze, data/silver e data/gold existem apenas durante a execução do job

-> Ao final do workflow, o runner é destruído

-> Nenhum arquivo Parquet é persistido automaticamente

Atualmente, no fluxo automatizado:

-> A camada Gold é carregada no Supabase (camada de serviço)

-> Os snapshots Silver não são persistidos entre execuções do CI

Impacto nas métricas históricas
---
Algumas métricas, como o proxy de emplacamento, dependem da comparação entre snapshots Silver de períodos diferentes.

No estado atual do projeto:

-> Esse tipo de métrica funciona plenamente em execução local

-> Em ambiente CI, a persistência histórica da Silver não está habilitada

Essa decisão é intencional, para manter o projeto:

-> simples,

-> fácil de entender,

-> focado em arquitetura e semântica corretas.

Evolução futura prevista
---
Como evolução natural do projeto, os snapshots Silver podem ser persistidos em uma camada de armazenamento externa (ex.: object storage), permitindo:

-> histórico completo em execuções automatizadas,

-> proxy de emplacamento totalmente funcional em CI,

-> separação ainda mais clara entre pipeline, storage e consumo.

Essa evolução não é requisito para o estado atual do projeto e está documentada como próximo passo arquitetural.

---
Autor: Matheus Gimenez