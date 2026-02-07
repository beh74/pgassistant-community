<p align="center">
  <img src="media/pgassistant_logo.png" alt="pgAssistant" height="120px"/>
  <br>
  <em>A PostgreSQL assistant for developers<br />
      Understand, optimize, and improve your PostgreSQL database with ease.</em>
  <br>
</p>

[![Documentation](https://img.shields.io/badge/Documentation-pgAssistant-blue?logo=readthedocs)](https://beh74.github.io/pgassistant-blog/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/license/mit)
[![Docker Pulls](https://img.shields.io/docker/pulls/bertrand73/pgassistant?label=Docker%20Pulls)](https://hub.docker.com/r/bertrand73/pgassistant)
![Image Size](https://img.shields.io/docker/image-size/bertrand73/pgassistant/latest)
![Latest version](https://img.shields.io/docker/v/bertrand73/pgassistant?sort=semver)

---

## About

**pgAssistant** is an open-source tool designed to help developers **understand** and **optimize** PostgreSQL database performance.  
It provides actionable insights into database behavior, detects schema issues, assists in query tuning, and even integrates with AI to go beyond traditional analysis.

Originally published under the [MIT License](LICENSE), this project is now **community-maintained**.  
The original public snapshot is archived [here (Wayback Machine)](https://web.archive.org/web/20250000000000*/https://github.com/nexsol-technologies/pgassistant).

---

## Demo Access

You can try the live demo here: [https://ov-004f8b.infomaniak.ch/](https://ov-004f8b.infomaniak.ch/)

To connect to the demo database, use the following credentials:

- **Connection URI** `postgresql://postgres:demo@demo-db:5432/northwind`

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


### ⚠️ Very Important Notice

- This demo **does not use any Large Language Model (LLM)**.  
  Do **not** provide any API keys or credentials in the *LLM Settings* form.

- **If you enter your own LLM provider API key in the demo**, please be aware that:  
  - The key **may be visible to other demo users**.  
  - The key **could be used by others without your consent**.  
  - **The demo administrator accepts no responsibility** for any misuse, exposure, or unauthorized access resulting from providing such keys.

- The demo database is **reset and reinitialized from scratch every day**.  
  All stored data will be permanently deleted during this process.

---

## Screenshots

| | | |
|---|---|---|
| <a href="media/dashboard.png?raw=1"><img src="media/dashboard.png" alt="Dashboard" width="300"></a> | <a href="media/myqueries.png?raw=1"><img src="media/myqueries.png" alt="Mes requêtes" width="300"></a> | <a href="media/top_queries.png?raw=1"><img src="media/top_queries.png" alt="Top queries" width="300"></a> |
| <a href="media/analyze_parameters.png?raw=1"><img src="media/analyze_parameters.png" alt="Analyze parameters" width="300"></a> | <a href="media/cache_usage.png?raw=1"><img src="media/cache_usage.png" alt="Cache usage" width="300"></a> | <a href="media/index_coverage.png?raw=1"><img src="media/index_coverage.png" alt="Index coverage" width="300"></a> |
| <a href="media/issue_fk_missing.png?raw=1"><img src="media/issue_fk_missing.png" alt="Foreign key missing" width="300"></a> | <a href="media/issue_redundant_idx.png?raw=1"><img src="media/issue_redundant_idx.png" alt="Index redondant" width="300"></a> | <a href="media/pgtune.png?raw=1"><img src="media/pgtune.png" alt="PgTune" width="300"></a> |
| <a href="media/pgtune_docker.png?raw=1"><img src="media/pgtune_docker.png" alt="PgTune Docker" width="300"></a> | <a href="media/pgrune_results.png?raw=1"><img src="media/pgrune_results.png" alt="PgRunE results" width="300"></a> | <a href="media/ddl.png?raw=1"><img src="media/ddl.png" alt="DDL Viewer" width="300"></a> |
| <a href="media/llm_prompt.png?raw=1"><img src="media/llm_prompt.png" alt="LLM Prompt" width="300"></a> | <a href="media/llm_guidelines.png?raw=1"><img src="media/llm_guidelines.png" alt="LLM Guidelines" width="300"></a> | <a href="media/llm_settings.png?raw=1"><img src="media/llm_settings.png" alt="LLM Settings" width="300"></a> |
| <a href="media/gpt-oss-2.png?raw=1"><img src="media/gpt-oss-2.png" alt="GPT OSS 2" width="300"></a> |  |  |

## Features

### Database Performance & Optimization
- **In-Depth Performance Analysis** – Understand exactly how your PostgreSQL instance behaves.  
- **Schema Optimization** – Detect and fix structural issues in your database.  
- **Top Query Insights** – Spot your heaviest queries, automatically suggest parameters , run EXPLAIN ANALYZE, and get an easy-to-understand breakdown of the results — enhanced by AI if you choose.
- **PGTune Integration** – Get recommended `ALTER SYSTEM` parameters tailored to your workload. Create a ready-to-use `docker-compose` file from PGTune results.  
- **Index & Cache Coverage**  – Measure index usage per table/column and identify queries with poor cache/index hit ratios.  

### Smarter SQL Management
- **Query Library** – Store and manage SQL queries in a JSON file for quick reuse.  
- **SQL Linting** – Keep your SQL clean with [sqlfluff](https://github.com/sqlfluff/sqlfluff).  
- **Common Value Detection** – Use `pg_stats` to discover the most frequent query parameter values.  

### AI-Powered Database Assistance
- **Compatibility** — pgAssistant is compatible with any LLM that supports the OpenAI API.
- **Local LLM Support** – Use Ollama or other locally hosted models seamlessly.  
- **Use AI to perform RFC compliance Checks** – Verify if table definitions comply with relevant RFCs.
- **Use AI to verify table compliance with your SQL guidelines** - Just provide a valid URL to your guidelines.   
- **Use AI to optimize a query** – AI can help you define new indexes or rewrite a query. 

Check this [post](https://beh74.github.io/pgassistant-blog/post/gpt-oss/) to try out the new open-source **gpt-oss** model integration with pgAssistant and Ollama.

If running an LLM locally isn’t an option for you, this [post](https://beh74.github.io/pgassistant-blog/post/pgassistant-on-swissdata/) might be worth checking out.


### Deployment
- **pgAssistant is Docker based** – Easy to deploy


---

## Quick Start

Choose your preferred setup method:

### Option A — Docker (recommended)

Follow the guide [Get Started with Docker](https://beh74.github.io/pgassistant-blog/doc/startup_docker/)


### Option B — Python (local environment)
Follow the guide [Get Started with Python](https://beh74.github.io/pgassistant-blog/doc/startup_python/)

---

## Documentation

Need help? Check the complete documentation and articles on the **[pgAssistant Blog](https://beh74.github.io/pgassistant-blog/)**.

RSS feed : https://beh74.github.io/pgassistant-blog/index.xml

---

## Mindset

Most database optimization tools are deterministic:  
they tell you what is slow, but not **why** it’s slow or how to **fix** it beyond metrics.

**pgAssistant** bridges that gap by combining deterministic analysis with the reasoning power of LLMs:  
- Check compliance against standards like RFCs  
- Suggest structural improvements such as adding missing primary keys  
- Offer optimization paths based on context, not just numbers  

**Note:** LLMs can make mistakes—sometimes big ones. Always validate suggestions, test extensively, and use pgAssistant in **non-production environments**.  
The goal is to make developers more autonomous, educated, and less dependent on DBA time.

---

## Special Thanks

Thanks to the creators of [Volt Bootstrap 5 Dashboard](https://github.com/themesberg/volt-bootstrap-5-dashboard) for the beautiful UI framework that powers pgAssistant’s interface.  
You saved me countless hours of front-end work!

---