# 🚕 Do Lixo ao Luxo: Limpe dados numa ETL PySpark + AWS

Bem-vindo ao repositório oficial do workshop!

Este projeto demonstra a construção de um pipeline de Engenharia de Dados **End-to-End** na AWS. Vamos pegar dados "sujos" de táxi de Nova York, processá-los com **PySpark** e transformá-los em insights valiosos (Dashboards), utilizando **Terraform** para subir toda a infraestrutura automaticamente.

O objetivo é aplicar na prática a arquitetura **Lakehouse (Medallion)**:
* 🟤 **Bronze (Raw):** Dados brutos ingeridos.
* ⚪ **Silver (Curated):** Dados limpos e tipados.
* 🟡 **Gold (Aggregated):** KPIs de negócio prontos para BI.

## 📂 Estrutura do Projeto

Para facilitar o entendimento, o projeto está dividido em:

* **`notebooks/`**: Contém o Jupyter Notebook (Google Colab) usado na demonstração ao vivo. É aqui que prototipamos, testamos a lógica e vemos os erros acontecendo "na cara".
* **`scripts/`**: Contém o código Python (`.py`) final, refatorado e pronto para produção. Este é o arquivo que o AWS Glue vai executar.
* **`terraform/`**: Contém a Infraestrutura como Código (IaC). Aqui definimos os recursos da AWS (S3, Glue Job, Crawler, IAM Roles) para não precisar clicar no console.

---

## 🏗 Arquitetura do Pipeline

1.  **Ingestão Automática:** O script verifica se o dado existe; se não, baixa da web (streaming) direto para o S3.
2.  **Processamento (Glue + PySpark):**
    * Leitura de arquivos Parquet.
    * Tratamento de *Schema Drift* (mudança de tipos).
    * Limpeza de nulos e regras de negócio.
3.  **Armazenamento (S3 + Delta Lake):** Dados salvos em formato Delta para garantir performance e ACID.
4.  **Consumo (Athena):** O Crawler cataloga os dados e o Athena permite consultas SQL.

**Stack:** AWS S3, AWS Glue, AWS Athena, Delta Lake, Terraform.

---

## 🚀 Como Rodar

### Pré-requisitos
* Conta AWS ativa.
* [AWS CLI](https://aws.amazon.com/cli/) instalado e configurado (`aws configure`).
* [Terraform](https://www.terraform.io/) instalado.

### Passo 1: Infraestrutura (Terraform)
Primeiro, vamos criar o "terreno" (Bucket, Permissões, Banco de Dados) na AWS.

No terminal, entre na pasta do Terraform:

```bash
cd terraform
````

Inicialize e aplique a infraestrutura:

```bash
# Baixa os plugins da AWS
terraform init

# Cria os recursos na sua conta
terraform apply
# Digite 'yes' quando solicitado
```

> **O que isso faz?** Cria um Bucket S3, sobe o script da pasta `scripts/` para lá, cria as permissões (IAM) e configura o Job do Glue.

### Passo 2: Executar o Pipeline (ETL)

Agora que a infraestrutura existe, vamos rodar o processamento.

1.  Vá ao console da AWS \> **AWS Glue** \> **ETL Jobs**.
2.  Selecione o job criado: `taxi-pipeline-job`.
3.  Clique em **Run**.
      * *O job fará o download dos dados, processará as camadas Bronze/Silver/Gold e salvará no S3.*

*(Opcional) Se a tabela não aparecer automaticamente:*

1.  Vá em **AWS Glue** \> **Crawlers**.
2.  Selecione `taxi-gold-crawler` e clique em **Run**.

### Passo 3: Analisar os Dados (Athena)

Vamos ver o "Luxo" (Gold) via SQL.

1.  Vá ao console do **AWS Athena**.
2.  Selecione o database `taxi_lakehouse_db`.
3.  Execute a query para descobrir quem paga melhor (Crédito vs. Dinheiro):

<!-- end list -->

```sql
SELECT 
    day_of_week AS dia_semana,
    is_credit_card AS forma_pagamento,
    total_rides AS total_corridas,
    avg_ticket AS ticket_medio,
    avg_tip_pct as indice_generosidade, -- Quem dá mais gorjeta?
    total_revenue AS faturamento_total
FROM "taxi_lakehouse_db"."financial_performance"
ORDER BY total_revenue DESC;
```

-----

### 🧹 Limpeza (Importante\!)

Para evitar cobranças na AWS após o workshop, destrua os recursos criados:

```bash
# Dentro da pasta terraform/
terraform destroy
# Digite 'yes' para confirmar
```

-----

*Projeto desenvolvido para fins educacionais - Workshop "Do Lixo ao Luxo".*