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

# Application Design
Upon execution of the application, it consumes various parameters. Currently the only mandatory input is at least one YAML file and/or directory containing at least one YAML file. This YAML file should contain at least one RQL query (see `query_data` for examples) to be executed and captured. Upon successful execution of the RQL query against Prisma Cloud, the data will be written to an external MySQL database.

## Development
For local development, Docker Compose is used. A local DB is stood up along with the application. Currently, the local development flow does not include reaching into Amazon Secretsmanager to retrieve secrets. This is currently done by filling in the `.env` and `db/password.txt` files with relevant information.

## Production
Docker Compose is not used in production. The application should be built as normal and pushed to a given registry to be consumed by a runner (serverless, etc.).
 