<p align="center">
  <img src="media/pgassistant_logo.png" alt="pgAssistant" height="120px"/>
</p>

<h1 align="center">pgAssistant</h1>

<p align="center">
  <strong>AI-powered PostgreSQL Performance & Schema Optimization Assistant</strong><br/>
  Diagnose, understand and optimize complex PostgreSQL workloads using real execution plans and full schema context.
</p>

<p align="center">
  <a href="https://beh74.github.io/pgassistant-blog/">
    <img src="https://img.shields.io/badge/Documentation-pgAssistant-blue?logo=readthedocs">
  </a>
  <a href="https://opensource.org/license/mit">
    <img src="https://img.shields.io/badge/License-MIT-green.svg">
  </a>
  <a href="https://hub.docker.com/r/bertrand73/pgassistant">
    <img src="https://img.shields.io/docker/pulls/bertrand73/pgassistant?label=Docker%20Pulls">
  </a>
  <img src="https://img.shields.io/docker/image-size/bertrand73/pgassistant/latest">
  <img src="https://img.shields.io/docker/v/bertrand73/pgassistant?sort=semver">
</p>

<p align="center">
⭐ If pgAssistant helps you, please consider starring the repository.
</p>

---

# What is pgAssistant?

pgAssistant is an open-source tool designed to help developers and DBAs **understand and optimize PostgreSQL databases** beyond traditional metric dashboards.

It combines:

- Deterministic database analysis  
- [Global Advisor](advisor.md) (deterministic, database-wide analysis)**  
- Query-level advisor based on real execution plans (`EXPLAIN ANALYZE`)  
- Full schema inspection (DDL) with automatic relational graph visualization to reveal table dependencies and structural issues  
- Structural issue detection (missing indexes, redundant indexes, missing foreign keys, datatype inconsistencies)  
- Naming convention and RFC(s) validation  
- Database parameters & statistics  
- Optional AI-assisted reasoning  

The goal: turn raw PostgreSQL internals into **actionable optimization decisions**.

---

## Changelog

Stay up to date with the latest features and improvements:  
[View the full changelog](CHANGELOG.md)

# Why pgAssistant is Different

Most tools show metrics.

pgAssistant provides **context-aware analysis**.

When using AI-assisted features, pgAssistant injects:

- Table definitions (DDL) with Index definitions
- Database configuration parameters
- `pg_stats` insights
- Real execution plans (`EXPLAIN ANALYZE`)
- Query text

This drastically reduces hallucination risk and enables meaningful recommendations such as:

- Composite index suggestions  
- Join strategy improvements  
- Parameter tuning (e.g., `work_mem`, `effective_cache_size`)  
- Schema corrections   

This is not “copy-paste your query into ChatGPT”.

It is **structured, contextualized database analysis**.

## Deterministic Analysis First, AI When Needed

Starting with **pgAssistant v2.8**, deterministic analysis is no longer a collection of isolated checks.

It is now unified into the **Global Advisor**.

### Global Advisor (One-Click Analysis)

The **Global Advisor** allows you to run a full database analysis **in one click**.

It aggregates all deterministic checks performed directly on PostgreSQL system catalogs and turns them into **prioritized, actionable recommendations**.

Each recommendation is enriched with:

- **Ranking** (what to fix first)  
- **Confidence level** (how reliable the finding is)  
- **Impact** (expected performance or maintainability gain)  
- **Effort** (estimated implementation cost)  

Typical detected issues include:

- Missing indexes on foreign keys  
- Datatype inconsistencies in relationships  
- Redundant or overlapping indexes  
- Unused indexes  
- Index coverage gaps  


This approach is:

- **Deterministic** → no randomness, no hallucination  
- **Consistent** → same input, same output  
- **Actionable** → directly usable in production workflows  

---

### AI-Assisted Analysis (Optional Layer)

AI is used as an **optional augmentation layer**, not a replacement.

It helps with:

- Query rewrite suggestions  
- Context-aware optimization reasoning  
- RFC compliance checks  
- Naming and convention recommendations  

---

pgAssistant’s philosophy is simple:

> **Start with deterministic truth.  
> Then use AI to go further.**

# Real-World Example

Complex 10-table join query.

Initial execution time: **3.2 seconds**

pgAssistant recommendations:

- Add 2 composite indexes  
- Rewrite a nested loop join  
- Adjust `work_mem`  
- Fix missing foreign key  

New execution time: **420 ms**

→ **7.6x improvement**

(Results depend on workload, always validate in non-production environments.)

---

# AI-Powered Database Assistance (Optional)

Compatible with Ollama or any OpenAI-compatible API.

- Query optimization suggestions  
- Index recommendation  
- SQL rewrite proposals  
- RFC compliance checks  
- Custom guideline validation (give a URL with your specific guidelines)  

AI is optional. pgAssistant remains fully usable without it.

---

# Screenshots

## Dashboard
![Dashboard](media/dashboard.png)

## Global advisor
![Global advisor](media/global_advisor.png)

## Query Insight
![Query Insight](media/analyze_query_insight.png)

## Query Insight Relational view
![Query Insight](media/analyze_relational.png)

## SQL Advisor
![SQL Advisor](media/analyze_advisor.png)

## AI Query Optimization
![LLM Optimize Query](media/llm_optimize_query.png)

## Schema Issue Detection
![Missing FK](media/issue_fk_missing.png)

(See `/media` folder for more screenshots.)

---

# Quick Start

## Option A — Docker (Recommended)

Follow the guide:  
https://beh74.github.io/pgassistant-blog/doc/startup_docker/

## Option B — Python (Local Environment)

https://beh74.github.io/pgassistant-blog/doc/startup_python/

---

# Live Demo

Try the demo:

https://ov-004f8b.infomaniak.ch/

Demo database connection: postgresql://postgres:demo@demo-db:5432/northwind

⚠️ The public demo does NOT use an LLM.  
⚠️ Do not provide personal API keys in the public demo.

If you want to try the new database report API coming with v2.0 :
```
curl -X POST https://ov-004f8b.infomaniak.ch/api/v1/report \
  -H "Content-Type: application/json" \
  -d '{
    "db_config": {
      "db_host": "demo-db",
      "db_port": 5432,
      "db_name": "northwind",
      "db_user": "postgres",
      "db_password": "demo"
    }
  }'
```

The demo database is reset daily.

---

# Who is pgAssistant for?

- Backend developers working with complex SQL  
- PostgreSQL DBAs : you can add your favorites secrets queries using the MyQueries feature 
- DevOps engineers diagnosing performance issues  
- Teams without dedicated DBA resources  
- Developers wanting to understand PostgreSQL internals more deeply  

---

# Philosophy

Traditional tools tell you *what* is slow.

pgAssistant helps you understand:

- Why it is slow  
- What to change  
- How to validate the change  

It combines deterministic PostgreSQL introspection with optional AI reasoning to make developers more autonomous.

LLMs can make mistakes.  
Always validate suggestions and test extensively before applying changes in production.

---

# Documentation & Blog

Full documentation:  
https://beh74.github.io/pgassistant-blog/

RSS feed:  
https://beh74.github.io/pgassistant-blog/index.xml

---

Contributions welcome.

---

# License

MIT License

---

# Acknowledgments

UI framework based on Volt Bootstrap 5 Dashboard:  
https://github.com/themesberg/volt-bootstrap-5-dashboard