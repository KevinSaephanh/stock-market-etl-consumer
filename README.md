# Stock Market ETL Consumer

This project does the following:
- Consume messages from a Kafka topic containing stock data (producer app [here](https://github.com/KevinSaephanh/stock-market-etl-producer))
- Transform the format with Pandas
- Save the modified data to AstraDB
- Export the modified data to a CSV file
- Upload the CSV file to AWS S3

Tech:
- Python
- Kafka
- AstraDB
- AWS