# Workshop 2: ETL Process Using Apache Airflow

This project involves extracting, transforming, and loading (ETL) data using **Apache Airflow**. The project specifically focuses on data from **Spotify** and **Grammy Awards**, which are stored in a MySQL database, cleaned, and merged for analysis. Finally, the data is visualized using **Looker**.

## Author
David Melo Valbuena

## Repository
[GitHub Link to the Repository](https://github.com/Davidmelo9133/WorkShop-2.git)

## Overview of the Project
The main goal of this project was to create an automated ETL pipeline using Airflow to handle data from two CSV files:
- `spotify_dataset.csv` (Spotify track data)
- `the_grammy_awards.csv` (Grammy Awards data)

### Process:
1. **Extract**: The data is extracted from the two CSV files.
2. **Transform**: Both datasets are cleaned and processed. Columns with null values are handled, and data is standardized for consistency.
3. **Load**: The cleaned data is loaded into MySQL tables.
4. **Merge**: The Spotify and Grammy data is merged into a new table called `Spotify_Merged_With_Grammys` for further analysis.

### Tools Used:
- **Apache Airflow**: For ETL pipeline automation.
- **MySQL**: To store and query the data.
- **Looker**: For data visualization and dashboard creation.

### Files Included:
- `workshop_dag_etl.py`: The DAG file for the ETL pipeline in Airflow.
- `spotify_dataset.csv`: Spotify dataset (used for ETL).
- `the_grammy_awards.csv`: Grammy Awards dataset (used for ETL).
- `requirements.txt`: Python dependencies for the project.
- `DashBoard.pdf`: Contains the final dashboard created using Looker.

## How to Run the Project

Follow these steps to run the project on your **WSL** (Windows Subsystem for Linux) environment.

### Prerequisites
Before running the project, ensure that you have the following installed:

1. **Python 3.8+**: Required to run Airflow and the ETL process.
2. **Apache Airflow**: Used to orchestrate the ETL pipeline.
3. **MySQL**: Used as the database for storing processed data.
4. **Looker**: For creating and visualizing the data dashboard (optional if using other visualization tools).
5. **WSL**: Make sure WSL is installed and configured properly on your Windows machine(Ubuntu).

# Step 1: Clone the Repository
# Clone the repository to your local WSL environment.

git clone https://github.com/Davidmelo9133/WorkShop-2.git
cd WorkShop-2

# Step 2: Set Up a Python Virtual Environment
# Create and activate a Python virtual environment to manage dependencies.

python3 -m venv myenv
source myenv/bin/activate

# Step 3: Install Dependencies
# Install all required Python libraries listed in requirements.txt:

pip install -r requirements.txt

# Step 4: Set Up MySQL Database on WSL
# Install MySQL inside your WSL environment if you haven't already:

sudo apt-get update
sudo apt-get install mysql-server

# Start the MySQL service:

sudo service mysql start

# Create a database for the project:

mysql -u root -p
CREATE DATABASE spotify_data;

# Step 5: Set Up Apache Airflow on WSL
# Install Apache Airflow:

pip install apache-airflow

# Initialize Airflow database:

airflow db init

# Set up Airflow User (if necessary):

airflow users create \
    --username admin \
    --firstname David \
    --lastname Melo \
    --role Admin \
    --email admin@example.com

# Set the environment variables for Airflow:

export AIRFLOW_HOME=~/airflow

# Start the Airflow web server:

airflow webserver --port 8080

# Start the Airflow scheduler:

airflow scheduler

# Step 6: Trigger the ETL DAG
# Navigate to the Airflow UI by opening your browser and going to:

http://localhost:8080

# Trigger the DAG:
# Find the DAG named workshop_dag_etl in the Airflow UI and click the "Trigger" button to run the ETL pipeline.

# Step 7: Visualize the Data in Looker
# After running the ETL process and loading the data into the MySQL database, follow these steps to connect Looker:

1. Open Looker.
2. Go to **Admin Panel** > **Connections**.
3. Click on **New Connection**.
4. For **Dialect**, select **MySQL**.
5. Fill in the connection details:
    - **Host**: `localhost`
    - **Port**: `3306`
    - **Database**: `spotify_data`
    - **Username**: `root`
    - **Password**: The password you set for your MySQL root user.
6. Test the connection to ensure everything is working.
7. Once connected, explore the database and start building visualizations based on the table `Spotify_Grammy_Joined`.

# Step 8: Build Visualizations in Looker
# Create insightful visualizations such as:
- Most popular album by year.
- Artists with the most Grammy wins.
- Correlation between song popularity and the number of instruments.
- The number of Grammy-winning songs that are longer than 3 minutes.

## Conclusion

This project successfully demonstrated the implementation of an ETL pipeline using Apache Airflow to extract, transform, and load data into a MySQL database. The datasets from Spotify and the Grammy Awards were merged and cleaned to provide valuable insights that were visualized using Looker. By automating the data ingestion and analysis processes, this pipeline helps streamline decision-making with clear and effective visualizations. The integration of WSL, Airflow, MySQL, and Looker showcases a robust and flexible environment for managing and analyzing large datasets. This approach highlights the importance of combining data sources to uncover new insights, ultimately enhancing the quality and impact of the analysis.
