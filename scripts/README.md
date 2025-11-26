# 🐍 Script ETL de Produção (AWS Glue)

Este diretório contém o código Python (`tlc_taxi_script.py`) responsável por todo o processamento de dados do nosso Lakehouse.

Diferente dos Notebooks (que usamos para prototipar), este script foi desenhado para ser **robusto, automatizado e escalável**, rodando dentro da infraestrutura serverless do AWS Glue.

## ⚙️ O que este script faz?

O script executa um pipeline **End-to-End** dividido em 4 etapas principais:

### 1. Ingestão Automática (Self-Healing)
Antes de iniciar o Spark, o script verifica se os dados brutos existem no S3.
* **Lógica:** Utiliza `boto3` e `requests` para baixar os arquivos Parquet de 2019 diretamente da fonte pública (CloudFront) via *stream* para o S3.
* **Benefício:** Se você deletar os dados do S3, o script baixa tudo de novo sozinho.

### 2. Camada Bronze (Raw)
* **Objetivo:** Trazer os dados para dentro do Delta Lake sem perder informações.
* **Ações:**
    * Define um Schema físico manual (para evitar erros de tipagem como Double vs Long).
    * Adiciona metadados de auditoria (`ingestion_date`, `source_file`).
    * Salva em formato **Delta** particionado por `year` e `month`.

### 3. Camada Silver (Curated)
* **Objetivo:** Limpeza, padronização e enriquecimento.
* **Transformações:**
    * **Rename:** Padroniza colunas para `snake_case` (ex: `VendorID` -> `vendor_id`).
    * **Drop Nulls:** Remove linhas críticas sem data ou valor.
    * **Type Casting:** Garante que números sejam números e datas sejam timestamps.
    * **Mapeamento (UDFs):** Traduz códigos numéricos (ex: `1`, `2`) para texto legível (ex: `Credit Card`, `Cash`).
    * **Feature Engineering:** Calcula a duração da viagem em minutos.
    * **Filtros de Negócio:** Remove viagens do futuro, valores negativos e erros de GPS.

### 4. Camada Gold (Aggregated)
* **Objetivo:** Responder perguntas de negócio (KPIs).
* **Transformações:**
    * Calcula o `% de Gorjeta` por corrida.
    * Agrega dados por **Dia da Semana** e **Tipo de Pagamento**.
    * Aplica **Window Functions** para calcular o *Market Share* diário de cada forma de pagamento.
* **Saída:** Registra a tabela `gold_financial` no **AWS Glue Catalog** para ser consultada via SQL no Athena.

---

## 🛠 Bibliotecas Utilizadas

* **`pyspark`**: Processamento distribuído de Big Data.
* **`delta`**: Framework de armazenamento (ACID, Time Travel, Schema Enforcement).
* **`boto3`**: SDK da AWS para interagir com o S3.
* **`requests`**: Para baixar dados da web.

## 📝 Como ler o código

O script está estruturado sequencialmente:
1.  **Setup:** Configurações de Spark e Delta.
2.  **Ingestão:** Função `ingest_raw_data`.
3.  **Bronze:** Leitura Parquet -> Escrita Delta.
4.  **Silver:** Leitura Bronze -> Limpeza -> Escrita Delta.
5.  **Gold:** Leitura Silver -> Agregação -> `saveAsTable`.

---
*Este script é executado automaticamente pelo Job `taxi-pipeline-job` criado pelo Terraform.*