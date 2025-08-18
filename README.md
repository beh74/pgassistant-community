<p align="center">
  <img src="media/pgassistant.png" alt="pgAssistant" height="120px"/>
  <br>
  <em>A PostgreSQL assistant for developers<br />
      Understand, optimize, and improve your PostgreSQL database with ease.</em>
  <br>
</p>

[![Documentation](https://img.shields.io/badge/Doumentation-pgAssistant-blue?logo=readthedocs)](https://beh74.github.io/pgassistant-blog/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/license/mit)
[![Docker Pulls](https://img.shields.io/docker/pulls/bertrand73/pgassistant?label=Docker%20Pulls)](https://hub.docker.com/r/bertrand73/pgassistant)
![Image Size](https://img.shields.io/docker/image-size/bertrand73/pgassistant/latest)
![Latest version](https://img.shields.io/docker/v/bertrand73/pgassistant?sort=semver)


## Table of Contents
1. [About](#about)
2. [Features](#features)
3. [Quick Start](#quick-start)
4. [Screenshots](#screenshots)
5. [Mindset](#mindset)
6. [Documentation](#documentation)
7. [Special Thanks](#special-thanks)

---

## About

**pgAssistant** is an open-source tool designed to help developers **understand** and **optimize** PostgreSQL database performance.  
It provides actionable insights into database behavior, detects schema issues, assists in query tuning, and even integrates with AI to go beyond traditional analysis.

Originally published under the [MIT License](LICENSE), this project is now **community-maintained**.  
The original public snapshot is archived [here (Wayback Machine)](https://web.archive.org/web/20250000000000*/https://github.com/nexsol-technologies/pgassistant).

---

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
- **OpenAI Integration** – Let GPT explain query plans and suggest optimizations.  
- **Local LLM Support** – Use Ollama or other locally hosted models seamlessly.  
- **RFC Compliance Checks** – Verify if table definitions comply with relevant RFCs.  
- **Primary Key Discovery** – Ask the LLM to suggest an optimal primary key when missing. 

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

### LLM integration

Check this [post](https://beh74.github.io/pgassistant-blog/post/gpt-oss/) to try out the new open-source **gpt-oss** model integration with pgAssistant and Ollama.

If running an LLM locally isn’t an option for you, this [post](https://beh74.github.io/pgassistant-blog/post/pgassistant-on-swissdata/) might be worth checking out.



## Screenshots

### Dashboard
<img src="media/dashboard.png" alt="Dashboard" height="640px"/>

---


### RFC(s) compliance**
<img src="media/gpt-oss-2.png" alt="Dashboard" height="640px"/>

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

## Documentation

Visit the [pgAssistant Blog](https://beh74.github.io/pgassistant-blog/) for documentation, guides, and updates.

---

## Special Thanks

Thanks to the creators of [Volt Bootstrap 5 Dashboard](https://github.com/themesberg/volt-bootstrap-5-dashboard) for the beautiful UI framework that powers pgAssistant’s interface.  
You saved me countless hours of front-end work!

---