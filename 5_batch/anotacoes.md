## 5.3.3 Preparing Yellow and Green Taxi Data

1. Create `download_data.sh`
2. Download files `./download_data.sh yellow 2021`
3. On folder `zcat data/raw/yellow/2021/01/yellow_tripdata_2021_01.csv.gz | head -n 10`

Contar o número de rows - `zcat data/raw/yellow/2021/01/yellow_tripdata_2021_01.csv.gz | wc -1`

4. depois de fazer o download de ambos os datasets para os dois anos, para visualizar a árvore da pasta `tree data`

Se não estiver instalada no pacote do ubuntu, instale-a

5. No mês 8 aparece um csv, ele removeu isso

6. Create schema for datasets on jupyter notebooks

7. get the schema with pandas. O pandas reconhece .gz file

8. Create spark dataframe

## 5.3.4 - SQL with Spark
Se for usar CSV, precisa usar o schema.
A vantagem de utilizar parquet é que ele salva todas as configurações, inclusive o schema certinho.

1. Adaptei o download_data.sh para baixar direto em parquet da internet
`./download_data.sh yellow 2020`

2. Combine dataframes
- Common columns between two datasets
- Rename the columns
- A ordem das colunas não está correta
- Para unir, é preciso saber de onde vem as respectivas colunas
- combinar em `df_trips_data` unionAll

resultado do professor
sevice_type | count
green | 2304517
yellow | 39649199


### 5.4.1 - Anatomy of a Spark Cluster
Spark cluster

- Master
- Executors

### 5.4.2 - GroupBy in Spark
Reshuffling

External merge sort
