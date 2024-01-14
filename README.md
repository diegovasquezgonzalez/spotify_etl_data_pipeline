# Unleashing the Rhythm of Data: Building a Spotify ETL Pipeline on AWS 
**Objectives:**
- Construct a fully automated ETL pipeline on AWS to extract valuable insights from Spotify's "Top Songs - Global" playlist.
- Streamline the process of capturing and analyzing artist, album, and song data, empowering data-driven music exploration.
- Create a robust foundation for future analytics and visualizations, unlocking a deeper understanding of music trends and preferences.
# Project Diagram:![ETL Pipeline Diagram](https://github.com/diegovasquezgonzalez/spotify_etl_data_pipeline/assets/113812579/75042fa7-7af1-4949-a150-431fd135ceaf)
# Key AWS Services at Play 
- **Amazon EventBridge:** The conductor of our pipeline, orchestrating tasks and ensuring timely data extraction.
- **AWS Lambda:** The heart of our ETL process, responsible for extracting raw data from the Spotify API and transforming it into structured formats.
- **Amazon S3:** Our data repository, securely storing both raw and transformed data for analysis and exploration.
- **AWS Glue:** The cartographer of our data landscape, discovering and cataloging data for seamless access and querying.
- **Amazon Athena:** Our interactive query engine, empowering us to ask insightful questions and uncover hidden patterns within the music.
# Pipeline Journey
1. **Data Extraction on Cue:** EventBridge initiates the process by triggering the spotify_api_data_extract Lambda function at a specified schedule.
2. **Spotify API Interlude:** The Lambda function interacts with the Spotify API to retrieve the latest "Top Songs - Global" playlist data.
3. **Storing Raw Rhythms on S3:** The extracted data is deposited into the raw_data folder within our S3 bucket, awaiting transformation.
4. **Data Transformation Symphony:** A new file in the raw_data folder activates the spotify_transformation_load_function Lambda, which skillfully extracts artist, album, and song data, storing it in structured formats within the transformed_data folder.
5. **Glue Mapping the Musical Landscape:** Glue Crawlers meticulously scan the transformed data, inferring schema and storing metadata in the Glue Data Catalog, creating a harmonious map of our music data universe.
6. **Athena's Melodic Queries:** With data organized and cataloged, we unleash the power of Athena to compose insightful queries and uncover captivating trends and patterns within the world of music.
