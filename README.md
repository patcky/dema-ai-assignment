# Dema AI Technical Test
This project is a data processing script that reads CSV files, validates the data against table schemas, and saves the processed data to a PostgreSQL database. It also logs any errors encountered during processing and the script execution duration.

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

## Prerequisites
You need to have the following installed:
- Docker

## Installing
Clone the repository to your local machine:
```bash
git clone https://github.com/yourusername/yourrepository.git
```

Navigate to the project directory:
```bash
cd yourrepository
```

Pull the dependant Docker images:
```bash
docker pull dpage/pgadmin4
docker pull postgres
docker pull python
```

Build the Docker image:
```bash
docker-compose build
```

## Usage
Run the Docker container:
```bash
docker-compose up -d
```

## Functionality
The main function in main.py performs the following steps:

- Retrieves environment variables and database connection details.
- Gets table schemas.
- Reads CSV files and processes the data.
- Validates the data against the table schema.
- Saves the processed data to the database.
- Saves any errors encountered during processing.
- Logs the script execution duration.

## Testing
### Unit tests
Unfortunately, due to limitations in time, I didn't finish implementing the tests for this project. I have written some tests in the tests.py file, but they are not working in an automated way and need to be refactored. If you install the requirements in a Python env, you should be able to run them with VSCode debug tool for Python. Given more time, I would like to have automated the tests to run with the Docker container and also added more tests.

### Manual testing with pgAdmin
To test the script manually, you can use pgAdmin to query the database and visualize the data. You can also use pgAdmin to view the logs and errors table to see any errors encountered during processing.

To access pgAdmin, open a web browser and go to http://localhost:5050. Log in with the credentials set in the .env file (PGADMIN_DEFAULT_EMAIL and PGADMIN_DEFAULT_PASSWORD).

Once logged in, you can create a new server connection (Add New Server) to the PostgreSQL database using the following details from the .env file:

- Name: example_server_name (or any name you prefer)
- Hostname: POSTGRES_HOST
- Port: POSTGRES_PORT
- Maintenance database: POSTGRES_DB + ENV
- Username: POSTGRES_USER
- Password: POSTGRES_PASSWORD

You can then query the tables in the database to view the processed data and any errors encountered during processing.

### Example queries
To see the errors encountered during processing:
```sql
SELECT * FROM company_schema.errors;
```
To see orders for products that are out of stock:
```sql
SELECT
  o.orderid,
  p.productid
FROM company_schema.orders o
JOIN company_schema.products p
	ON p.productid = o.productid
WHERE p.quantity = 0
ORDER BY o.datetime DESC;
```
To see the number of orders for each product category:
```sql
SELECT
  p.category,
  count(o.orderid) as orders
FROM company_schema.orders o
JOIN company_schema.products p
	ON p.productid = o.productid
GROUP BY p.category
ORDER BY orders DESC;
```

## Technical decisions
### Language:
I chose to use Python because it is a versatile language that is easy to read and write and has a wide range of libraries and frameworks that make it easy to build applications, specifically for data processing.

### Containerization:
I chose to use Docker because it is a lightweight containerization tool that makes it easy to deploy applications without worrying about dependencies.

### Database:
I chose to use PostgreSQL as database because it is a powerful open-source relational database management system that is widely used (and I have experience with it).

I created the schema and tables using a separate init.sql file because it is a best practice to separate the database schema from the application code. Usually, in production environment, the database, schema and tables would be created manually or using a tool such as terraform.

I added the database connection details to the .env file because it is a best practice to store sensitive information such as database credentials in environment variables.

I included indexes on the tables to improve query performance, especially for large datasets.

I also included a table for errors, which stores any errors encountered during processing, to make it easier to identify and fix issues, without compromising the integrity of the data.

For database querying and visualization, I used pgAdmin, which is a popular open-source administration and development platform for PostgreSQL.

### Libraries:
To access the database without having to manually write queries, I used the SQLAlchemy ORM, which provides a high-level interface for interacting with the database.

To facilitate schema validation, I used panderas library, which provides a simple and efficient way to validate data against table schemas.

To read and write CSV files, I used the pandas library, which provides a high-level interface for working with data in tabular format.

To log errors and script execution duration, I used the logging module, which is a built-in module that makes it easy to log messages to a file or console.

### Performance vs data validation:
I chose to focus on error handling, data validation and logging because these are critical aspects of data processing that can help identify and fix issues quickly.

This means that the script may be slower than it would be if I had focused on performance. It is, of course, an arbitrary decision and could be changed depending on the requirements of the project.

As an example, for larger datasets the script could be optimized to use bulk inserts instead of inserting one row at a time. This would mean that the script would be faster, but it would also mean that if there was an error, it would be harder to identify and fix. It could also mean that if there was an error, the entire batch of data would be lost, rather than just the row that caused the error.

If performance was a critical requirement, I would have used a different approach, such as using a batch processing framework, with multithreading or multiprocessing, to process the data in parallel. Another possibility would be to build a data streaming pipeline, to process the data in real-time, therefore reducing the amount of data that needs to be processed at once.

Everything is a trade-off. :)

## Improvements
- Fix the tests in the tests.py file and add more tests.
- Refactor the code to make it more efficient.
- Add more error handling.
- Add a way to rerun the script without having to restart the container and automated retry/restart for failures.
- Add a way to monitor the script execution duration and errors in real-time (such as a dashboard or an automated tool with support for alerts).
- Add a way to automatically scale the script to handle larger datasets.
- Add a data retention policy to automatically delete old data from the database.
- Add a way to automatically backup the database.
- Add a way to automatically restore the database from a backup.
- Use a better tool to create the database schema and tables (such as terraform).

And many, many more...

Any suggestions are very welcome!
