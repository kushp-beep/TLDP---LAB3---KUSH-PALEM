#ETL Pipline

Description: The pipline extracts data from the Stack Overflow database from Google BigQuery, transforms it, and then loads it into the terminal and a local CSV file for viewers to easily read the data.

How to Run: You must create a Google Cloud Platform account to be able to run the program. You must set it up and connect it to your IDE, such as VSCode. You will also need to install some packages: google-cloud-bigquery, pandas, pyarrow. Then, run the program in the terminal and you will be able to see the output.

Pipeline Architecture: 
Extract --> extracts the data. In this case, it extracts the data from the Stack Overflow database from Google BigQuery.
Transform --> transforms the data. In this case, it transforms the data into a table-like format so it is easy to read.
Load --> loads the data. In this case, the data gets loaded into the terminal and also gets saved into a local CSV file.

Screenshots:
![BigQuery](BigQuery.png)
![Pipeline Terminal](pipeline_terminal.png)

Questions:
Q1:npm has the highest engag    ement score of 1.27. It is not that surprising as developers frequently encounter dependency issues.
Q2: Windoes has the highest unanswered rate of 36.04%. This is becasue the window's error messages are not always accurate because of how different each computer can be in terms of system configuration and installed software.
Q3:Big Query can query billions of rows of data, making it a good place to store extremely large sets of data. A local CSV is limited based on the machine's RAM and processing power. There is not enough memory on the local CSV to read large amounts of data. 
Q4:If I was a developer advocate, I would create content surrounding Next.js and VScode. This is becasue they both have high engagement when it comes to developers. 