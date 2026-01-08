frota-veiculos-brasil

Projeto de engenharia de dados para análise da frota de veículos do Brasil a partir de dados do RENAVAM, com pipeline automatizado, histórico por snapshots e decisões semânticas explícitas.
_____________________________________________________________________________________________________________________________________
Objetivo do Projeto

Construir um pipeline robusto e reprodutível para análise da frota de veículos no Brasil, seguindo boas práticas de engenharia de dados e separação clara de responsabilidades entre ingestão, semântica, analítica e consumo.
_____________________________________________________________________________________________________________________________________
Princípio central do projeto:

Pipeline gera a verdade → Banco replica → BI consome

Visão Geral da Arquitetura

Fluxo completo dos dados:

CKAN (RENAVAM)
→ Bronze (ingestão bruta)
→ Silver (tratamento semântico)
→ Gold (métricas analíticas)
→ Parquet (outputs)
→ PostgreSQL / Supabase (camada de serviço)
→ Power BI (consumo analítico)
_____________________________________________________________________________________________________________________________________
Arquitetura por Camadas

Bronze — Ingestão

Responsável exclusivamente por:

Consumo de dados via CKAN (RENAVAM)

Download de arquivos CSV/ZIP em memória

Padronização mínima de colunas (lowercase, underscore)

Persistência dos dados brutos em Parquet

Regras:

Nenhuma semântica

Nenhuma validação de negócio

Nenhuma agregação

Aceita o dado exatamente como fornecido
_____________________________________________________________________________________________________________________________________

Silver — Semântica

Responsável por:

Limpeza estrutural

Padronização textual (upper, sem acento)

Mapeamento explícito das colunas reais do dataset

Aplicação de regras semânticas

Validação de schema canônico

Enriquecimento leve permitido (ex: idade do veículo)

Todas as decisões de negócio vivem nesta camada.
_____________________________________________________________________________________________________________________________________

Gold — Analítica

Responsável apenas por métricas analíticas, sem qualquer nova inferência semântica.

Métricas geradas:

Perfil da frota por UF

Concentração de modelos

Penetração de montadoras

Penetração de modelos

Maturidade da frota por município

Proxy de emplacamento via comparação de snapshots
_____________________________________________________________________________________________________________________________________

Dimensões Semânticas

Conceito

As dimensões representam regras de negócio e contratos semânticos, não dados analíticos.

Elas existem para:

padronizar valores textuais inconsistentes

controlar domínios (ex: UF)

garantir reprodutibilidade das métricas

tornar decisões de negócio explícitas e auditáveis

Por esse motivo, as dimensões são versionadas no GitHub.

Como as dimensões foram criadas

O processo ocorreu em duas etapas:

1. Modelagem inicial em PostgreSQL local

Inicialmente, as dimensões foram criadas como tabelas em um PostgreSQL local, com o objetivo de:

formalizar a lógica de classificação

testar regras de normalização

deixar explícitas as decisões semânticas

Exemplos:

Dimensão de UF: mapeamento entre texto original e sigla canônica

Dimensão de montadoras: múltiplas regras de correspondência para uma mesma montadora

Cada linha representava uma regra, não um registro analítico.

2. Migração para arquivos Parquet versionados

Após validar o conceito, as dimensões foram extraídas do banco e persistidas como arquivos Parquet em:

data/dimensions/

Arquivos:

dim_uf.parquet

dim_montadoras.parquet

Esses arquivos passaram a ser:

parte do código do projeto

versionados no GitHub

independentes de banco de dados

O PostgreSQL deixou de ser dependência do pipeline para aplicação das regras.

Uso das dimensões no pipeline

As dimensões são consumidas na camada Silver para:

validação de valores

classificação semântica

padronização controlada

A camada Gold consome apenas dados já tratados, sem aplicar novas regras.
_____________________________________________________________________________________________________________________________________

Banco de Dados (Supabase)

O PostgreSQL (Supabase) é utilizado exclusivamente como camada de serviço.

Características:

Não é fonte de verdade

Pode ser recriado a qualquer momento

Recebe dados já consolidados da camada Gold

Serve dados para ferramentas de BI

Se o banco for perdido, todo o ambiente pode ser reconstruído a partir do pipeline.
_____________________________________________________________________________________________________________________________________

Power BI

O Power BI consome os dados diretamente do PostgreSQL/Supabase.

Características:

Não consome dados do GitHub

Não consome arquivos Parquet diretamente

Utiliza o banco como camada intermediária estável
_____________________________________________________________________________________________________________________________________

Versionamento (GitHub)

O repositório versiona apenas o que define comportamento e regras do pipeline.

Versionado

Código do pipeline (Bronze, Silver, Gold, Orchestrator)

GitHub Actions

README e documentação

requirements.txt

Dimensões semânticas (data/dimensions/*.parquet)

Não versionado

Dados brutos

Snapshots históricos

Outputs analíticos da camada Gold

Arquivos Parquet gerados por execução

Ambiente virtual (.venv)

Regra adotada:

Dados não são versionados.
Regras e contratos semânticos são versionados.

Estrutura do Repositório
frota-veiculos-brasil/
├── .github/workflows/
│   └── pipeline.yml
├── src/
│   ├── bronze_ingestion_ckan.py
│   ├── silver_processing.py
│   ├── gold_metrics.py
│   └── Orchestrator.py
├── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── dimensions/
│   │   ├── dim_uf.parquet
│   │   └── dim_montadoras.parquet
│   └── metadata/
├── scripts/
├── README.md
├── requirements.txt
└── .gitignore