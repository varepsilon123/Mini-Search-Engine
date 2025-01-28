# Mini-Search-Engine

## Installation

To install the required dependencies, run the following command:

```bash
pip install -r requirements.txt
```

## Running the Crawler

To run the crawler, use the following command:

```bash
scrapy crawl website_spider
```

## Database Connection

To connect to the database, you need to download the CA certificates. Run the following command:

```bash
curl --create-dirs -o $HOME/.postgresql/root.crt 'https://cockroachlabs.cloud/clusters/928ba3a6-9973-40e6-883a-125edc5f29ae/cert'
```

Set up the database URL environment variable:

```bash
export DATABASE_URL="cockroachdb://<SQL-USER-NAME>:<ENTER-SQL-USER-PASSWORD>@paula-the-crawler-7529.j77.aws-us-west-2.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"
```

Right now I am using a .env file