Na tarefa 3 - tentando rodar o dbt encontrei o seguinte problema:

```output
2025-01-29 14:52:35.488

[Date: 2025-01-29T17:52:35.488423Z] [Thread: MainThread] [Type: MainEncounteredError] Encountered an error:
 Database Error
 could not translate host name "postgres_zoomcamp" to address: Name or service not known
 ```

No README do módulo, já havia esse alerta:
If you're using Linux, you might encounter Connection Refused errors when connecting to the Postgres DB from within Kestra. This is because host.docker.internal works differently on Linux. Using the modified Docker Compose file below, you can run both Kestra and its dedicated Postgres DB, as well as the Postgres DB for the exercises all together. You can access it within Kestra by referring to the container name postgres_zoomcamp instead of host.docker.internal in pluginDefaults. This applies to pgAdmin as well. If you'd prefer to keep it in separate Docker Compose files, you'll need to setup a Docker network so that they can communicate with each other.

Então tive que criar uma network pra rodar direitinho.

---
Backfill

Inicialmente, eu tinha 4373861 observações na table green_trips
Iam de março a agosto / 2019
No backfill, pedi pra incluir as observações de janeiro e fevereiro do mesmo ano.

Não foram incluídos os dados, ops

No stage temos 575685

Percebi que subiu no outro database, o anterior mesmo.

--- Caiu a conexão, então fiz o docker-compose down e reiniciei

Voltei no db kestra, pra visualizar as yellow trips
total rows: 6299354
com observações de janeiro e fevereiro de 2020

