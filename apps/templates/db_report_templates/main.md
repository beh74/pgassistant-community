# pgAssistant -  Database report

Report date : {{ now.strftime("%Y-%m-%d %H:%M:%S") }}


## Database summary

- Database name : **{{ db_config["db_name"] }}**
- User : **{{ db_config["db_user"] }}**
- Host : **{{ db_config["db_host"] }}**
- Port : **{{ db_config["db_port"] }}**
---
