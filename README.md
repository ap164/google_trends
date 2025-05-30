# Google Trends Data Engineering & Business Intelligence

*🇵🇱 Polish version below*

## Project Description  
This project demonstrates a comprehensive process for acquiring, processing, and analyzing data on the popularity of search queries in Google Search. Data is retrieved using the PyTrends library (an unofficial Google Trends API) and processed through automated Apache Airflow pipelines that schedule and execute the entire ETL workflow. The results are stored in a PostgreSQL relational database and finally visualized in interactive dashboards created with Apache Superset, enabling in-depth trend analysis. The project is fully built using open-source tools.

## Google Trends Data Source  
Google Trends provides historical data on the frequency of specific search terms entered in Google Search, segmented by time, region, and category. It enables the analysis of changes in search popularity, allowing users to monitor market and social trends.

## Technologies Used  
- **Python** – ETL processes and PyTrends integration  
- **Apache Airflow** – task orchestration and ETL pipelines  
- **Apache Superset** – interactive dashboards and data visualization  
- **PostgreSQL** – relational database  
- **Docker & Docker Compose** – environment containerization  
- **SQL** – data operations and report queries  
- **Git** – version control  

### Python Libraries  
- **Pandas** – data frame manipulation, including validation and transformation  
- **Unittest / Pytest** – unit testing of functions and logic  
- **Pydantic, logging, os, yaml, email** – configuration management, environment variable handling, logging, and alert notifications  

## Pipeline Logic  
The Apache Airflow pipeline dynamically generates DAGs for two types of Google Trends data (`interest_over_time` and `interest_by_region`), with scheduling based on a `config.yaml` file. Each DAG runs the `run_etl` function, which retrieves data for individual keywords and performs ETL processing according to configuration parameters such as geographic location, category and query type.

<img width="981" alt="airflow-screen" src="https://github.com/user-attachments/assets/f523915b-0860-4d9f-bb7f-3c4d7cf78947" />


## Handling Data Source Limitations  
- PyTrends queries are executed sequentially to avoid Google’s 429 rate limit errors.  
- Keywords are grouped and processed by dedicated DAGs with different schedules.  
- Configuration validation is implemented using Pydantic.  
- Automatic retry logic is applied only to keywords that triggered a 429 error.  
- Issues are reported via email notifications to enable prompt response.  

## Visualization  
Data stored in PostgreSQL is presented through Apache Superset as interactive dashboards and reports, allowing detailed analysis of trends.
An example dashboard created to analyze trends in data visualization tools (Power BI, Looker, Superset, Tableau), helping to identify popularity patterns over time and across different regions:

<img width="981" alt="superset-screen" src="https://github.com/user-attachments/assets/afa6efc6-97aa-410c-a7cf-d83d8d9fa9f2" />


## Summary
It is a fully open-source data pipeline project that collects and analyzes search trends from Google Trends using Python, PyTrends, Apache Airflow, and PostgreSQL. Results are visualized with Apache Superset in interactive dashboards for easy trend exploration by time and region.




# PL: 

## Opis projektu  
Projekt prezentuje kompleksowy proces pozyskiwania, przetwarzania oraz analizy danych o popularności zapytań w wyszukiwarce Google. Dane są pobierane za pomocą biblioteki PyTrends (nieoficjalne API Google Trends) i przetwarzane w zautomatyzowanych pipeline’ach Apache Airflow, które harmonogramują i wykonują cały proces ETL. Wyniki zapisywane są w relacyjnej bazie PostgreSQL, a następnie wizualizowane w interaktywnych dashboardach tworzonych w Apache Superset, umożliwiając dogłębną analizę trendów wyszukiwań. Projekt jest w pełni oparty na narzędziach open-source.

## Źródło danych Google Trends  
Google Trends to narzędzie udostępniające historyczne dane o częstotliwości wyszukiwań określonych słów kluczowych w wyszukiwarce Google, z podziałem na regiony i kategorie. Pozwala na analizę zmian popularności zapytań oraz monitorowanie trendów.

## Użyte technologie  
- **Python** – ETL, integracja z PyTrends  
- **Apache Airflow** – orkiestracja zadań i pipeline’ów ETL  
- **Apache Superset** – interaktywne dashboardy i wizualizacje  
- **PostgreSQL** – baza danych  
- **Docker & Docker Compose** – konteneryzacja środowisk  
- **SQL** – operacje na danych i kod raportów  
- **Git** – kontrola wersji  

### Biblioteki Python  
- **Pandas** – operacje na dataframe’ach, m.in. walidacja i transformacja danych  
- **Unittest / Pytest** – testy jednostkowe funkcji i logiki  
- **Pydantic, logging, os, yaml, email** – zarządzanie plikami konfiguracyjnymi, odczyt zmiennych środowiskowych, tworzenie logów oraz wysyłanie alertów  

## Logika pipeline’u  
Pipeline w Apache Airflow dynamicznie tworzy DAG-i dla dwóch typów danych Google Trends (`interest_over_time`, `interest_by_region`), z harmonogramem opartym na pliku `config.yaml`. Każdy DAG wywołuje funkcję `run_etl`, która pobiera dane dla kolejnych słów kluczowych i wykonuje proces ETL zgodnie z parametrami konfiguracji, takimi jak geolokalizacja, kategoria czy interesujący okres czasu.

<img width="981" alt="airflow-screen" src="https://github.com/user-attachments/assets/f523915b-0860-4d9f-bb7f-3c4d7cf78947" />


## Obsługa ograniczeń źródła danych  
- Zapytania do PyTrends wykonywane są sekwencyjnie, aby uniknąć błędu 429 od Google.  
- Słowa kluczowe są grupowane i obsługiwane przez dedykowane DAG-i z różnymi harmonogramami.  
- Wdrożono walidację konfiguracji za pomocą Pydantic.  
- Mechanizm automatycznego ponawiania zapytań działa tylko dla słów, które spowodowały błąd 429.  
- Problemy są automatycznie zgłaszane mailowo, umożliwiając szybką reakcję.  

## Wizualizacja  
Dane z PostgreSQL są prezentowane w Apache Superset w postaci interaktywnych dashboardów i raportów, umożliwiających szczegółową analizę trendów oraz danych regionalnych.
Przykładowy dashboard stworzony do analizy trendów popularności narzędzi do wizualizacji danych (Power BI, Looker, Superset, Tableau), umożliwiający identyfikację wzorców zainteresowania w czasie oraz w różnych regionach:

<img width="981" alt="superset-screen" src="https://github.com/user-attachments/assets/afa6efc6-97aa-410c-a7cf-d83d8d9fa9f2" />


## Podsumowanie
Jest to projekt oparty na narzędziach open-source, pobierający i analizujący trendy wyszukiwań z Google Trends za pomocą Python, PyTrends, Apache Airflow i PostgreSQL. Wyniki prezentowane są w interaktywnych dashboardach w Apache Superset – umożliwiających analizę trendów w czasie oraz w różnych regionach.


#GoogleTrends #DataEngineering #BusinessIntelligence #ApacheAirflow #ApacheSuperset #PostgreSQL #ETL #OpenSource #DataPipeline #Python #Docker #SQL #TrendAnalysis #DataVisualization #DataAnalytics
