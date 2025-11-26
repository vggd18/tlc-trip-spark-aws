# 📓 Notebook Interativo: Do Lixo ao Luxo

Este diretório contém o **Jupyter Notebook** utilizado durante o workshop para demonstrar o funcionamento do PySpark e do Delta Lake passo a passo.

Diferente do script de produção (que roda "às cegas" na AWS), aqui nós vemos os dados, analisamos os schemas, encontramos erros e validamos as transformações em tempo real.

## 🎯 Objetivo

Simular um ambiente de desenvolvimento local onde o Engenheiro de Dados prototipa a lógica antes de implantar na nuvem.

Você vai aprender a:
1.  **Configurar** um ambiente Spark + Delta Lake do zero (no Google Colab).
2.  **Baixar** dados públicos via script.
3.  **Ingerir (Bronze):** Salvar dados brutos em formato Delta.
4.  **Limpar (Silver):** Aplicar regras de qualidade, remover nulos e tratar tipagem.
5.  **Agregar (Gold):** Criar inteligência de negócio e KPIs financeiros.

---

## 🚀 Como Rodar (Google Colab)

A maneira mais fácil de executar este notebook é usando o **Google Colab**, pois ele oferece recursos computacionais gratuitos na nuvem.

### Passo 1: Abrir o Notebook
1.  Faça o upload do arquivo `.ipynb` deste diretório para o seu Google Drive ou abra diretamente via GitHub.
2.  Ou clique no botão abaixo (caso o repositório seja público):

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/16g0E_xl-tSsA4dvcdn24H-6K0yLm76I7?usp=sharing)

### Passo 2: Instalação de Dependências
A primeira célula do notebook instala o necessário:
```python
!pip install pyspark==3.5.1 delta-spark==3.2.0 -q
````

### Passo 3: Execução

Vá rodando as células sequencialmente (Shift + Enter).

  * O notebook fará o download dos dados de táxi (aprox. 500MB) para o disco temporário do Colab.
  * Ele criará as pastas `lakehouse/bronze`, `lakehouse/silver` e `lakehouse/gold` localmente.

-----

## 🧪 O que estamos testando aqui?

### 1\. A Sujeira (Data Profiling)

Usamos comandos como `df.describe()` e `printSchema()` para identificar:

  * Colunas com tipos errados (ex: números lidos como texto).
  * Valores nulos críticos.
  * Dados incoerentes (ex: ano 2090 ou pagamento negativo).

### 2\. A Lógica de Transformação

Testamos as **UDFs (User Defined Functions)** e os dicionários de mapeamento para garantir que os códigos de pagamento (`1`, `2`) virem textos legíveis (`Credit Card`, `Cash`).

### 3\. Validação SQL

No final, registramos uma *Temp View* no Spark para rodar SQL e validar se os números batem:

```sql
SELECT ... FROM gold_financial ORDER BY total_revenue DESC
```

-----

## ⚠️ Diferenças para o Script de Produção (`scripts/`)

| Feature | Notebook (`.ipynb`) | Script Glue (`.py`) |
| :--- | :--- | :--- |
| **Ambiente** | Local / Google Colab | AWS Glue (Serverless) |
| **Armazenamento** | Disco Temporário / Local | Amazon S3 |
| **Execução** | Interativa (Célula a Célula) | Batch (Job Inteiro) |
| **Catálogo** | Memória (Spark Warehouse) | AWS Glue Data Catalog |

> **Nota:** O código aqui é idêntico em *lógica* ao script da pasta `scripts/`, mas adaptado para rodar localmente sem precisar de credenciais da AWS.