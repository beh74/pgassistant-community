import os
import requests
import markdown
import pymdownx
import re

from urllib.parse import urlparse, urlunparse

from openai import OpenAI
from .ddl import generate_tables_ddl
from .sqlhelper import get_tables
from .config import get_config_value
from .database import get_pg_tune_parameter
from .database import fetch_table_stats


def extract_root_uri(uri):
    """
    Extracts the root URI: scheme + host + port, discarding any path/query.

    Args:
        uri (str): A full URI (e.g., http://localhost:11434/v1/chat)

    Returns:
        str: The root URI (e.g., http://localhost:11434/)
    """
    parsed = urlparse(uri)
    root = urlunparse((parsed.scheme, parsed.netloc, '/', '', '', ''))
    return root

def check_ollama_status(base_uri=None, timeout=2):
    """
    Checks whether an Ollama server is running at the specified base URI.

    Args:
        base_uri (str): URI to check. If None, uses the LOCAL_LLM_URI env variable.
        timeout (int): Timeout for the HTTP request in seconds.

    Returns:
        str: One of:
            - "ollama"       → Ollama is running
            - "not_ollama"   → Server is reachable but does not appear to be Ollama
            - "unreachable"  → Failed to connect to the server
    """
    base_uri = base_uri or get_config_value('LOCAL_LLM_URI')
    if not base_uri:
        raise ValueError("No URI provided and LOCAL_LLM_URI is not set.")

    root_uri = extract_root_uri(base_uri)

    try:
        response = requests.get(root_uri, timeout=timeout)
        if response.status_code == 200 and "Ollama is running" in response.text:
            return "ollama"
        else:
            return "not_ollama"
    except requests.RequestException:
        return "unreachable"
    
def fix_code_blocks(text: str) -> str:
    # 1. Remove any language specifier after ``` (e.g. ```sql → ```)
    #text = re.sub(r'```[a-zA-Z0-9_+-]*', '```', text)

    # 2. Ensure every opened block is closed
    opens = len(re.findall(r'```', text))
    if opens % 2 != 0:  # odd number of fences → add closing fence
        text += "\n```"
    
    # 3. Systematically add a newline before each code fence
    text = re.sub(r'```', r'\n```', text)

    return text

def query_chatgpt(question):
    """
    Sends a question to ChatGPT or Ollama (depending on LOCAL_LLM_URI),
    and returns an HTML-rendered response.

    :param question: The user's question to send to the LLM.
    :return: HTML-formatted Markdown response from the model.
    """
    api_key = get_config_value('OPENAI_API_KEY', None)
    local_llm = get_config_value('LOCAL_LLM_URI', None)
    model_llm = get_config_value('OPENAI_API_MODEL', None)

    # Detect if LOCAL_LLM_URI points to an Ollama server
    use_ollama = local_llm and check_ollama_status(local_llm) == "ollama"

    if use_ollama:
        # Use Ollama's native API
        print("⚙️ Using native Ollama API")

        response = requests.post(
            f"{extract_root_uri(local_llm)}api/generate",
            json={
                "model": model_llm,
                "prompt": f"You are a Postgresql database expert.\n\nUser: {question}",
                "stream": False,
                "temperature": 0.2,
            },
            timeout=600
        )

        if response.status_code != 200:
            raise Exception(f"Ollama API error: {response.status_code} - {response.text}")

        output = response.json().get("response", "")
    else:
        # Use OpenAI-compatible API
        if not api_key:
            raise Exception("The environment variable OPENAI_API_KEY is not set. Cannot use OpenAI API.")

        print("⚙️ Using OpenAI API")

        if local_llm:
            client = OpenAI(api_key=api_key, base_url=local_llm)
        else:
            client = OpenAI(api_key=api_key)

        completion = client.chat.completions.create(
            model=model_llm,
            messages=[
                {"role": "system", "content": "You are a Postgresql database expert"},
                {"role": "user", "content": question}
            ],
            temperature=0.2,
            max_tokens=2000,
            frequency_penalty=0.1,
            presence_penalty=0.6,
            n=1
        )
        output = completion.choices[0].message.content

    
    md_text = fix_code_blocks(output)
    
    html = markdown.markdown(
            md_text,
            extensions=[
                "pymdownx.superfences",
                "pymdownx.highlight",
                "extra",
            ],
            extension_configs={
                "pymdownx.highlight": {
                    "use_pygments": True,
                    "guess_lang": False,
                    "linenums": False,
                },
                "pymdownx.superfences": {
                  
                },
            },
            output_format="html5",
        )
    return html


import re
import json
from typing import Any, Dict, Iterable, Optional, Union

def _strip_explain(sql: str) -> str:
    """
    Retire un préfixe EXPLAIN / EXPLAIN (options) [ANALYZE] s'il est présent.
    Garde la requête d'origine pour l'analyse.
    """
    pattern = r"""
        ^\s*EXPLAIN            # mot-clé
        (?:\s*\( (?: [^()]+ | \([^()]*\) )* \) )?  # options éventuelles, équilibrées
        (?:\s+ANALYZE)?        # ANALYZE optionnel (si pas dans la liste d'options)
        \s+                    # au moins un espace avant la vraie requête
    """
    return re.sub(pattern, "", sql, flags=re.IGNORECASE | re.VERBOSE)

def _plan_block(rows: Union[str, Dict[str, Any], Iterable[Dict[str, Any]]]) -> str:
    """
    Normalise la section plan :
    - si dict/list -> JSON pretty
    - si list de dicts avec 'QUERY PLAN' (comme psql) -> concatène
    - si str -> renvoie tel quel
    """
    if rows is None:
        return "_No plan provided_"
    if isinstance(rows, (dict, list)):
        return "```json\n" + json.dumps(rows, indent=2) + "\n```"
    if isinstance(rows, str):
        rows = rows.strip()
        fence = "```" if not rows.startswith("```") else ""
        return f"{fence}\n{rows}\n{fence}"
    # list de dicts (format psql)
    try:
        lines = []
        for r in rows:  # type: ignore[assignment]
            if isinstance(r, dict) and "QUERY PLAN" in r:
                lines.append(str(r["QUERY PLAN"]))
        return "```\n" + "\n".join(lines) + "\n```"
    except Exception:
        return "```\n" + str(rows) + "\n```"


def get_llm_query_for_query_analyze(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    sql_query: str,
    rows: Union[str, Dict[str, Any], Iterable[Dict[str, Any]]],
    *,

    db_config: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Generates a robust prompt for PostgreSQL query optimization.
    Automatically integrates server parameters via get_pg_tune_parameter()
    """
    # 1) SQL 
    original_sql = _strip_explain(sql_query)
    tables = get_tables(original_sql)

    # 2) DDL
    ddl = generate_tables_ddl(host, port, database, user, password, tables)
    ddl_block = f"```sql\n{ddl}\n```" if ddl else "_DDL unavailable_"

    # 3) Server parameters
    effective_version = None
    effective_settings = None
    
    try:
        cfg = db_config 
        running_values, major = get_pg_tune_parameter(cfg)
        
        if effective_settings is None:
            effective_settings = running_values or {}
        if effective_version is None:
            effective_version = str(major)
    except Exception as e:
        print(f"get_pg_tune_parameter failed: {e}")
        pass

    # 4) Readable context
    meta_lines = []
    
    if effective_version:
        meta_lines.append(f"- PostgreSQL version: **{effective_version}**")
    if effective_settings:
        pretty = "\n".join(f"  - {k}: {v}" for k, v in effective_settings.items())
        meta_lines.append("- Server settings (subset):\n" + pretty)
    
    table_stats=fetch_table_stats(db_config, tables) if db_config else None
    if table_stats:
        try:
            stats_pretty = []
            for t, s in table_stats.items():
                parts = []
                for k in ("estimated_rows", "n_live_tup", "n_dead_tup", "last_analyze"):
                    if k in s and s[k] is not None:
                        parts.append(f"{k}={s[k]}")
                stats_pretty.append(f"  - {t}: " + ", ".join(parts) if parts else f"  - {t}")
            meta_lines.append("- Table stats (subset):\n" + "\n".join(stats_pretty))
        except Exception:
            pass

    plan_section = "\n".join(row['QUERY PLAN'] for row in rows)

    # 5) Final prompt (identical to the “improved” version, with the context above)
    llm = []
    llm.append(
        "You are a **senior PostgreSQL query optimizer**. "
        "Read *all* sections before answering. Only use the information provided. "
        "If a required piece of info is missing, say **Insufficient information** and list what is missing."
    )
    if meta_lines:
        llm.append("\n**Context**:\n" + "\n".join(meta_lines))
    llm.append("\n**1) DDL of involved tables**\n" + ddl_block)
    llm.append("\n**2) SQL query (original, without EXPLAIN)**\n```sql\n" + original_sql.strip() + "\n```")
    llm.append(
        "\n**3) EXPLAIN ANALYZE output**\n"
        "Be careful with indentation and node nesting.\n"
        + plan_section
    )
    llm.append(
        """
**Rules (very important):**
- Do **not** recommend indexes that already exist in the DDL. Primary keys are already indexed.
- If you recommend an index, include: table, columns (with order), predicate (if partial), opclass (if non-default), and whether `CONCURRENTLY` is advisable.
- Never assume extensions (e.g., `pg_trgm`, `btree_gin`) are available unless visible in the DDL; if needed, say it's a *conditional* recommendation.
- If no meaningful improvement is likely, say **No change required** and explain why.
- Cite specific plan evidence for each recommendation (e.g., misestimation, Hash Join spill, Seq Scan on high-selectivity predicate, Sort method=external).
- You may also propose **query rewrites** (equivalent semantics) to improve plan selection (e.g., pushdown of predicates, aligning ORDER BY with an index), and explain the expected plan change
- Keep all SQL **PostgreSQL-valid** (match the version if provided).
"""
    )
    llm.append(
    """
**Respond in this exact Markdown structure:**

1. **Summary of Findings**
   - 3–6 bullet points. Mention bottlenecks with node names and evidence (rows, loops, time, buffers, spill/WAL if present).

2. **Recommendations (ranked)**
   For each item:
   - *Action:* one line title
   - *SQL (if applicable):* a single fenced block with ready-to-run statements
   - *Impact:* High/Medium/Low
   - *Confidence:* High/Medium/Low
   - *Why:* short justification pointing to DDL/plan evidence

   **If no immediate improvement is found, include at least one _Conditional (Scale-up / What-if)_ recommendation with explicit assumptions.**

3. **Justification & Trade-offs**
   - Why the planner chose the current strategy; what changes your proposal triggers (e.g., join order, index usage, memory).

4. **If information is missing**
   - Bullet list of the minimal extra data needed (e.g., `ANALYZE` freshness, `work_mem`, `n_distinct`, histograms from `pg_stats`).
"""
)
    llm.append("\n**Reminder:** Avoid redundant or unnecessary index recommendations. Verify against the DDL above.")
    return "\n".join(llm)


def generate_primary_key_prompt(table_name: str, ddl: str) -> str:
    """
    Generates a prompt for an LLM to determine the best primary key for a PostgreSQL table
    and provide the necessary ALTER TABLE command.

    Args:
        table_name (str): The name of the table.
        ddl (str): The DDL (Data Definition Language) statement for the table.

    Returns:
        str: A formatted prompt for the LLM.
    """
    prompt = f"""
I have a PostgreSQL table that does not have a primary key. In some cases, a natural key (like an ISO country code) is a good choice, 
but when no stable unique column exists, a technical key (`SERIAL` or `UUID`) is preferable.

A technical key is best when:
- No single column or combination of columns is reliably unique.
- Natural keys are too large, unstable, or inefficient for indexing.
- Composite keys make queries and relationships complex.
- The table is large, requiring fast lookups and indexing.
- The system is distributed and needs globally unique identifiers.

Given the following table structure, suggest the most appropriate primary key (either an existing column or a new technical key) and explain why.

Additionally, provide the necessary **ALTER TABLE** SQL command(s) to implement your suggested primary key.

**One more thing** : Can you also check if the column types and their lengths in the following table are appropriate based on their names and potential usage ? Please mention RFC conventions if any exist end provide an ALTER command to change the column data type or length.

**Table Name:** {table_name}  
**DDL:**  
```sql
{ddl}
```
"""
    return prompt

def analyze_table_format (ddl: str) -> str:
    llm_prompt = f"""
# 📌 SQL Table Structure Validation Based on RFC & International Standards (PostgreSQL Compatible)

## **Task**  
You are an expert in **database design**, **SQL optimization**, and **data standards**. Your goal is to **validate the structure of a SQL table (DDL)** based on relevant **RFCs, international standards, and best practices**.  

## **Instructions**  
1. **Analyze the given DDL statement** and verify whether it adheres to **established standards and best practices** in different domains, including:  
   - **Networking & Web** (RFCs for emails, domain names, and addresses)  
   - **Healthcare** (FHIR, HL7)  
   - **E-commerce & Invoicing** (UBL, UN/CEFACT, EDIFACT)  
   - **Finance & Payments** (ISO 20022, IBAN, BIC, SWIFT, PCI-DSS)  
   - **Geolocation** (ISO 3166 for country codes, ISO 6709 for geolocation)  
   - **Personal Data & Identity** (ISO 5218 for gender, ISO 27799 for health privacy, OIDC/SAML for identity management)  
   - **Date & Time** (ISO 8601)  
   - **Languages & Localization** (ISO 639 for language codes, ISO 4217 for currencies)  
   - **Database Best Practices** (Normalization, indexing, constraints)  
2. **Validate column types, sizes, constraints, and indexes**, ensuring compliance with:  
   - **RFC 5322** for **email addresses**  
   - **RFC 6350** for **names and addresses (vCard format)**  
   - **RFC 3696** for **name and domain validation**  
   - **FHIR (HL7 Fast Healthcare Interoperability Resources)** for **medical data**  
   - **UBL (Universal Business Language)** for **invoices and orders**  
   - **ISO 20022** for **financial transactions**  
   - **ISO 3166** for **country codes**  
   - **ISO 5218** for **gender classification**  
   - **ISO 4217** for **currency codes**  
   - **ISO 8601** for **date and time formats**  
   - **E.164** for **phone number formatting**  
3. **Propose improvements**:  
   - Adjust **column data types or sizes** if necessary  
   - Add missing **constraints** (e.g., `NOT NULL`, `UNIQUE`, `CHECK`)  
   - Optimize **indexing strategies** for better performance  
4. **Generate SQL `ALTER TABLE` statements**, ensuring **100% PostgreSQL compatibility**:  
   - **Use PostgreSQL syntax for altering column types** (`ALTER COLUMN ... SET DATA TYPE`)  
   - **Use `ADD CONSTRAINT ... CHECK(...)` for validations**  
   - **Use `CREATE INDEX` to improve search performance**  
5. **Provide justifications** for each recommended change based on relevant standards.  

---

## **Here is the DDL to analyze**  
```sql
{ddl}
```
"""
    return llm_prompt


def get_llm_query_for_query_optimize (sql_query):
    llm = (
        "Could you optimize this postgresql query for me : \n "
        f"{sql_query}\n"
    )
    return llm

def list_available_models():
    """
    Returns a list of available models from the OpenAI API or a compatible API (like Ollama).
    - Uses OPENAI_API_KEY if available.
    - Otherwise, uses LOCAL_LLM_URI with a dummy 'none' API key.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("LOCAL_LLM_URI")

    if not api_key and not base_url:
        raise ValueError("Neither OPENAI_API_KEY nor LOCAL_LLM_URI is set.")

    # Initialize the OpenAI client
    client = OpenAI(
        api_key=api_key or "none",  # "none" works for Ollama or APIs without authentication
        base_url=base_url or "https://api.openai.com/v1"
    )

    try:
        models = client.models.list()
        return [model.id for model in models.data]
    except Exception as e:
        print(f"❌ Error fetching models: {e}")
        return []