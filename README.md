# Pipeline ETL — E-commerce Analytics

Pipeline de dados ponta a ponta (Bronze → Silver → Gold) para o time de Dados de um e-commerce em crescimento. O objetivo é consolidar receita, performance de entrega e consistência de pedidos em tabelas analíticas prontas para consumo.

---

## Sumário

1. [Estrutura do Projeto](#estrutura-do-projeto)
2. [Arquitetura das Camadas](#arquitetura-das-camadas)
3. [Modelo de Dados — Gold](#modelo-de-dados--gold)
4. [Qualidade de Dados](#qualidade-de-dados)
5. [Conectores de Fonte](#conectores-de-fonte)
6. [Como Executar](#como-executar)
7. [Dicionário de Dados](#dicionário-de-dados)
8. [Dependências](#dependências)

---

## Estrutura do Projeto

```
Case_ZetaDados/
│
├── main.py                      # Orquestrador — Bronze → Silver → Gold
├── requirements.txt
│
├── config/
│   ├── settings.py              # Caminhos, constantes de negócio (LATE_DELIVERY_HOURS)
│   └── schemas.py               # 14 schemas Pandera (Bronze · Silver · Gold)
│
├── connectors/                  # Fontes de dados (baixo acoplamento)
│   ├── base_connector.py        # ABC: extract() → DataFrame
│   ├── csv_connector.py         # Arquivos CSV locais
│   ├── api_connector.py         # REST API (paginação suportada)
│   └── db_connector.py          # SQL via SQLAlchemy
│
├── pipeline/
│   ├── base.py                  # LayerProcessor ABC (validate + write)
│   ├── bronze/ingestor.py       # Ingestão raw + metadados + validação tolerante
│   ├── silver/cleaner.py        # Union + transformações + dedup + validação estrita
│   └── gold/builder.py          # Joins + KPIs + tabelas fato/dimensão
│
├── validators/
│   └── schema_validator.py      # Wrapper Pandera (modo tolerante vs. estrito)
│
├── utils/
│   └── io.py                    # read_csv / write_csv
│
├── docs/
│   ├── generate_catalog.py      # Gera data_dictionary.xlsx
│   └── data_dictionary.xlsx     # Dicionário de dados (gerado)
│
└── data/
    ├── bronze/
    │   └── ingest_date=YYYY-MM-DD/
    │       ├── orders.csv
    │       ├── order_items.csv
    │       ├── shipments.csv
    │       ├── customers.csv
    │       └── products.csv
    ├── silver/
    │   ├── orders.csv
    │   ├── order_items.csv
    │   ├── shipments.csv
    │   ├── customers.csv
    │   └── products.csv
    └── gold/
        ├── fact_orders.csv
        ├── fact_order_items.csv
        ├── dim_customers.csv
        └── dim_products.csv
```

---

## Arquitetura das Camadas

```
[Fonte CSV / API / Banco]
          │
          │  BaseConnector.extract()
          ▼
┌──────────────────────────────────────────────────────┐
│  BRONZE  (pipeline/bronze/ingestor.py)               │
│                                                      │
│  • Dados ingeridos sem transformação                 │
│  • Metadados adicionados: _ingest_date,              │
│    _source_file, _load_ts                            │
│  • Validação Pandera tolerante                       │
│    (erros são logados, dados preservados)            │
│                                                      │
│  Saída: data/bronze/ingest_date=YYYY-MM-DD/          │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  SILVER  (pipeline/silver/cleaner.py)                │
│                                                      │
│  • Leitura do Bronze do dia                          │
│  • Union com Silver existente                        │
│  • Transformações:                                   │
│    - Parse de timestamps (formatos mistos)           │
│    - Correção de decimais BR ("0,00" → 0.0)          │
│    - Normalização de strings (UF, status, carrier)   │
│    - Tratamento de "N/A" → NaT                       │
│    - Cálculo de item_net_amount                      │
│  • Deduplicação por PK (keep='last')                 │
│  • Validação Pandera estrita (erros levantam         │
│    exceção)                                          │
│                                                      │
│  Saída: data/silver/<tabela>.csv                     │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  GOLD  (pipeline/gold/builder.py)                    │
│                                                      │
│  • Joins entre tabelas Silver                        │
│  • Agregações (desconto total, net_amount)           │
│  • KPIs de entrega:                                  │
│    - delivery_time_hours                             │
│    - is_late (> 72h, configurável)                   │
│  • Tabelas fato e dimensões                          │
│  • Validação Pandera estrita                         │
│                                                      │
│  Saída: data/gold/<tabela>.csv                       │
└──────────────────────────────────────────────────────┘
```

---

## Modelo de Dados — Gold

### Diagrama de relacionamento

```
dim_customers ──────────────────────────────────────────┐
  customer_id (PK)                                      │
  state, city, created_ts                               │
                                                        │ FK
                                              ┌─────────▼──────────┐
dim_products ─────────────────────────────────►   fact_orders       │
  product_id (PK)         FK ◄───────────────┤   order_id (PK)     │
  category, brand, created_ts                │   customer_id        │
                          │                  │   order_date         │
                          │                  │   gross_amount       │
                          │                  │   discount_total     │
                          │                  │   net_amount         │
                          │                  │   payment_method     │
                          │                  │   status_final       │
                          │                  │   carrier            │
                          │                  │   shipping_cost      │
                          │                  │   delivery_time_hours│
                          │                  │   is_late            │
                          │                  └─────────┬────────────┘
                          │                            │ FK
                          │                  ┌─────────▼────────────┐
                          └──────────────────► fact_order_items      │
                                             │   order_id (PK, FK)  │
                                             │   product_id (PK, FK)│
                                             │   quantity           │
                                             │   unit_price         │
                                             │   discount_amount    │
                                             │   item_net_amount    │
                                             └──────────────────────┘
```

### Tabelas Gold

| Tabela | Grain | Linhas esperadas | Finalidade |
|--------|-------|-----------------|-----------|
| `fact_orders` | 1 por pedido | = # pedidos únicos | Receita, status, KPIs de entrega |
| `fact_order_items` | 1 por (pedido, produto) | = # itens | Receita por SKU |
| `dim_customers` | 1 por cliente | = # clientes únicos | Segmentação por UF/cidade |
| `dim_products` | 1 por produto | = # produtos únicos | Análise por categoria/marca |

---

## Qualidade de Dados

### Problemas identificados na fonte e tratamentos

| Problema | Tabela / Coluna | Tratamento |
|----------|----------------|-----------|
| Timestamp nulo | `orders.order_ts` | Mantido como NaT; `order_date` = NaT |
| Formato misto de data (`DD/MM/YYYY` e `YYYY-MM-DD`) | `orders.order_ts`, `shipments.shipped_ts`, `products.created_ts` | `pd.to_datetime(dayfirst=True, errors='coerce')` |
| Decimal com vírgula (`"0,00"`) | `shipments.shipping_cost` | `str.replace(",", ".")` antes do `to_numeric` |
| Valor `"N/A"` em campo de data | `shipments.delivered_ts` | Regex replace → `NaT` |
| Casing inconsistente de UF (`"Rj"` → `"RJ"`) | `customers.state` | `.str.upper()` |
| Espaços extras em string | `shipments.carrier`, demais | `.str.strip()` |
| Carrier faltante | `shipments.carrier` | Aceito como nulo (pedido pode não ter carrier) |
| Duplicidade incremental | Todas | Dedup por PK em Silver com `keep='last'` |

### Modos de validação Pandera

| Camada | Modo | Comportamento em falha |
|--------|------|----------------------|
| Bronze | `tolerant` | Loga o erro; mantém a linha no dataset |
| Silver | `strict` | Lança exceção; interrompe o pipeline |
| Gold | `strict` | Lança exceção; interrompe o pipeline |

---

## Conectores de Fonte

O `BronzeIngestor` aceita qualquer classe que implemente `BaseConnector`:

```python
from connectors.csv_connector import CSVConnector
from connectors.api_connector import APIConnector
from connectors.db_connector  import DBConnector

# CSV local
connector = CSVConnector("Archives/ingest_date=2025-12-01/orders.csv")

# REST API paginada
connector = APIConnector(
    url="https://api.example.com/orders",
    headers={"Authorization": "Bearer <token>"},
    data_key="results",
    paginate=True,
)

# Banco de dados (PostgreSQL, MySQL, SQLite, SQL Server…)
connector = DBConnector(
    connection_string="postgresql+psycopg2://user:pass@host/db",
    query="SELECT * FROM orders WHERE DATE(order_ts) = :dt",
    params={"dt": "2025-12-01"},
)

# Usar o conector no ingestor
from pipeline.bronze.ingestor import BronzeIngestor
BronzeIngestor(connector, ingest_date="2025-12-01").run("orders")
```

---

## Como Executar

### 1. Instalar dependências

```bash
pip install -r requirements.txt
```

### 2. Executar o pipeline para uma data

```bash
python main.py --source Archives/ingest_date=2025-12-01 --date 2025-12-01
```

### 3. Executar múltiplos dias (incremental)

```bash
python main.py --source Archives/ingest_date=2025-12-01 --date 2025-12-01
python main.py --source Archives/ingest_date=2025-12-02 --date 2025-12-02
```

A cada execução, o Silver faz union com o arquivo existente e deduplica. O Gold é sempre reconstruído do zero a partir do Silver consolidado.

### 4. Executar via Python

```python
from main import run_pipeline

run_pipeline(
    source_dir="Archives/ingest_date=2025-12-01",
    ingest_date="2025-12-01",
)
```

---

## Dicionário de Dados

Para gerar o dicionário de dados completo em Excel:

```bash
python docs/generate_catalog.py
```

Saída: `docs/data_dictionary.xlsx` com 3 abas:

| Aba | Conteúdo |
|-----|---------|
| **Visão Geral** | Uma linha por tabela: layer, grain, chave primária, descrição, caminho |
| **Colunas** | Uma linha por coluna: tipo, nullable, PK, descrição, validação, origem, exemplo |
| **Qualidade** | Perfil de nulos por coluna lido dos CSVs Silver/Gold (se existirem) |

---

## Dependências

| Pacote | Uso |
|--------|-----|
| `pandas >= 2.0` | Manipulação de dados em todas as camadas |
| `pandera >= 0.18` | Validação de schemas (Bronze / Silver / Gold) |
| `requests` | `APIConnector` |
| `sqlalchemy >= 2.0` | `DBConnector` |
| `openpyxl` | Exportação do dicionário de dados em Excel |
| `python-dateutil` | Parsing robusto de timestamps |

---

## Regras de Negócio Configuráveis

Edite `config/settings.py` para ajustar:

```python
# Pedidos com delivery_time_hours > este valor são marcados como is_late = True
LATE_DELIVERY_HOURS: int = 72
```
