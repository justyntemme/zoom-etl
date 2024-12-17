# zoom-etl

# Architecture & Infrastructure
TODO

## Connect to Prisma Cloud
To successfully pull data from Prisma Cloud utilizing this solution, you must create a `.env` file in the root directory. The contents should be as follows:

```
PRISMA_PCPI_NAME=""
PRISMA_PCPI_ID=""
PRISMA_PCPI_SECRET=""
PRISMA_PCPI_URL=""
PRISMA_PCPI_VERIFY=""
```

Failure to create this file and populate it correctly will prevent successful connection to Prisma Cloud and pulling data.

## Database Connection

Below are settings that must exist within the `.env` file to properly connect to the SQL database in a number of scenarios (fake data generation, testing, and in the main application.)

```
# Name of the Database
DB_NAME=""
# User to connect to the db as
DB_USER=""
# Password for the db user
DB_PASSWORD=""
DB_PORT="3306"
DB_HOST="127.0.0.1"
# Name of the table in the db
DB_TABLE=""
```

# Application Design
Upon execution of the application, it consumes various parameters. Currently the only mandatory input is at least one YAML file and/or directory containing at least one YAML file. This YAML file should contain at least one RQL query (see `query_data` for examples) to be executed and captured. Upon successful execution of the RQL query against Prisma Cloud, the data will be written to an external MySQL database.

## Development
For local development, Docker Compose is used. A local DB is stood up along with the application. Currently, the local development flow does not include reaching into Amazon Secretsmanager to retrieve secrets. This is currently done by filling in the `.env` and `db/password.txt` files with relevant information.

## Production
Docker Compose is not used in production. The application should be built as normal and pushed to a given registry to be consumed by a runner (serverless, etc.).
 
# Tools

## Fake Data Generator
A fake data generator was created to generate data which can populate a given database to calculate performance metrics. The script `lib/tools/fake_data_generator.py` currently takes a number of parameters to write to a `.csv` output file.

## Write Data to SQL
This tool just bulk writes fake data (or real) to a given database (ensure `.env` has correct `DB_` settings.)