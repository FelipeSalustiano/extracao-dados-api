# Pipeline Extração de Dados — Gastos Públicos (API do Brasil.io)

Este projeto implementa um pipeline de **extração, armazenamento e organização** dos dados públicos de **gastos diretos do governo federal**, disponibilizados pela API do [Brasil.io](https://brasil.io/).

O objetivo é tornar esses dados mais acessíveis, estruturados e preparados para **análises exploratórias, visualizações e futuros modelos preditivos**.

---

## Arquitetura de Dados

| Camada      | Formato | Descrição |
|-------------|---------|-----------|
| **Raw**     | JSON    | Dados brutos armazenados página a página, exatamente como retornados pela API. |
| **Bronze**  | Parquet | Dados organizados e particionados por **ano** e **mês**, otimizados para leitura rápida e análise. |

---

## Funcionamento do Pipeline

1. A função `data_api()` acessa a API utilizando um token de autorização.
2. Os dados são baixados em blocos (páginas) e salvos na pasta `dataset/raw/`.
3. Todos os registros são consolidados em um DataFrame único.
4. O DataFrame é convertido para **Parquet** e salvo em `dataset/bronze/`, já particionado por `ano` e `mes`.
5. Essa estrutura facilita consultas eficientes e integrações com ferramentas como **Power BI**, **Spark**, **Athena**, etc.

---

## Estrutura de Pastas
extracao-dados-api
  - dataset
      - raw 
      - bronze
      - gold 
      - silver 
  - venv 
  - explicacao-codigo.txt
  - main.py
