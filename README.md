# AWS Data Lakehouse: NYC Taxi Pipeline 🚖

Este projeto demonstra a construção de um pipeline de Engenharia de Dados **End-to-End** na AWS, utilizando **Terraform** para Infraestrutura como Código (IaC) e **PySpark** no AWS Glue para processamento de Big Data.

O objetivo é ingerir, processar e analisar dados públicos de corridas de táxi de Nova York (2019), aplicando a arquitetura **Medallion (Bronze, Silver, Gold)**.

## 🏗 Arquitetura

1.  **Ingestão Automática:** O script baixa dados públicos da web e salva no S3 (Streaming).
2.  **Bronze (Raw):** Ingestão bruta em formato **Delta Lake** particionado.
3.  **Silver (Curated):** Limpeza de dados, tipagem forte, remoção de nulos e tratamento de colunas.
4.  **Gold (Aggregated):** Agregações de negócio (Faturamento, Gorjetas) prontas para BI.
5.  **Consumption:** Consultas SQL via **AWS Athena**.

**Stack Tecnológica:** AWS S3, AWS Glue, AWS Athena, Delta Lake, Terraform.

## 🚀 Como Rodar

### Pré-requisitos
* Conta AWS ativa.
* [AWS CLI](https://aws.amazon.com/cli/) configurado (`aws configure`).
* [Terraform](https://www.terraform.io/) instalado.

### Passo 1: Infraestrutura (Terraform)
Na pasta raiz do projeto:

```bash
# Inicializa os plugins do Terraform
terraform init

# Planeja e aplica a infraestrutura (S3, IAM Roles, Glue Job)
terraform apply
# Digite 'yes' quando solicitado
````

### Passo 2: Executar o Pipeline (ETL)

1.  Vá ao console da AWS \> **AWS Glue** \> **ETL Jobs**.
2.  Selecione o job `taxi-pipeline-job`.
3.  Clique em **Run**.
      * *O job fará o download automático dos dados, processará as 3 camadas e registrará as tabelas.*

### Passo 3: Analisar os Dados (Athena)

1.  Vá ao console do **AWS Athena**.
2.  Selecione o database `taxi_lakehouse_db`.
3.  Execute a query de exemplo:

<!-- end list -->

```sql
SELECT 
    day_of_week,
    is_credit_card,
    avg_tip_pct as generosity_index,
    total_revenue
FROM "taxi_lakehouse_db"."gold_financial"
ORDER BY total_revenue DESC;
```

### 🧹 Limpeza (Destruir Recursos)

Para evitar cobranças na AWS, ao finalizar os estudos, destrua a infraestrutura:

```bash
terraform destroy
```

-----

*Projeto desenvolvido para fins educacionais.*