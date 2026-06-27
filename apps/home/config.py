import os
import json
import requests

CONFIG_PATH = "config.json"
ENV_KEYS = [
    "LOCAL_LLM_URI",
    "OPENAI_API_KEY",
    "OPENAI_API_MODEL",
    "LLM_SQL_GUIDELINES",
    "LLM_TABLE_RFC_PROMPT_TEMPLATE",
    "LLM_TABLE_NAMING_PROMPT_TEMPLATE",
]

def init_or_load_env(config_path=CONFIG_PATH, keys=ENV_KEYS):
    """
    If the config.json file exists, load its values into os.environ.
    If it doesn't exist, create it from the current os.environ values.
    """
    if os.path.exists(config_path):
        # Load and apply existing config
        with open(config_path, "r") as f:
            config = json.load(f)
        for key, value in config.items():
            os.environ[key] = value
       
    else:
        # Create config from current environment
        config = {key: os.environ.get(key, "") for key in keys}
        with open(config_path, "w") as f:
            json.dump(config, f, indent=4)


def update_llm_config(
    llm_uri=None,
    llm_api_key=None,
    llm_model=None,
    config_path=CONFIG_PATH,
    llm_sql_guidelines=None,
    llm_table_rfc_prompt_template=None,
    llm_table_naming_prompt_template=None,
):
    """
    Updates the LLM configuration in config.json with the given values.
    Creates the file if it doesn't exist.

    Args:
        llm_uri (str): New value for LOCAL_LLM_URI
        llm_api_key (str): New value for OPENAI_API_KEY
        llm_model (str): New value for OPENAI_API_MODEL
        config_path (str): Path to the JSON config file (default: config.json)
        llm_sql_guidelines (str): a valid URL for SQL guidelines (http or https)
        llm_table_rfc_prompt_template (str): Prompt template for RFC table analysis
        llm_table_naming_prompt_template (str): Prompt template for SQL naming analysis
    """
    config = {}

    # Load existing config if it exists
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

    # Default value for guidelines URL
    default_guidelines = (
        "https://raw.githubusercontent.com/beh74/pgassistant-blog/refs/heads/main/content/post/sql-guide.md"
    )

    # Validate URL if provided
    if llm_sql_guidelines is not None and llm_sql_guidelines.strip() != "":
        if llm_sql_guidelines.startswith(("http://", "https://")):
            try:
                response = requests.get(llm_sql_guidelines, timeout=10)
                if response.status_code >= 400:
                    raise ValueError(
                        f"URL not accessible (HTTP {response.status_code}): {llm_sql_guidelines}"
                    )
            except requests.RequestException as e:
                raise ConnectionError(
                    f"Unable to reach the SQL guidelines URL within 10s: {llm_sql_guidelines}\n→ {e}"
                )
        else:
            raise ValueError(
                f"Invalid URL format for llm_sql_guidelines: {llm_sql_guidelines}"
            )
        config["LLM_SQL_GUIDELINES"] = llm_sql_guidelines
    else:
        config["LLM_SQL_GUIDELINES"] = default_guidelines

    # Update other values if provided
    if llm_uri is not None:
        config["LOCAL_LLM_URI"] = llm_uri
    if llm_api_key is not None:
        config["OPENAI_API_KEY"] = llm_api_key
    if llm_model is not None:
        config["OPENAI_API_MODEL"] = llm_model
    if llm_table_rfc_prompt_template is not None:
        config["LLM_TABLE_RFC_PROMPT_TEMPLATE"] = llm_table_rfc_prompt_template
    if llm_table_naming_prompt_template is not None:
        config["LLM_TABLE_NAMING_PROMPT_TEMPLATE"] = llm_table_naming_prompt_template

    # Write back to file
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)


def get_config_value(key, default=""):
    """
    Returns the value of a given key from the config.json file.

    Args:
        key (str): The config key to look up.
        default (str): The default value if key is not found.

    Returns:
        str: The value from config.json or the default.
    """
    if not os.path.exists(CONFIG_PATH):
        return default

    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    return config.get(key, default)
