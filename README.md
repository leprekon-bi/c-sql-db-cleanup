# 1C SQL DB Cleanup

A Python project for cleaning up your database.

## Getting Started

### 1. Install Python

Download and install Python 3.10 or newer from the [official Python website](https://www.python.org/downloads/).  
Make sure to check the box "Add Python to PATH" during installation.

### 2. Install SQL Server ODBC Driver

- **Windows:**  
  Download and install the [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).
- **macOS/Linux:**  
  Follow the instructions for your OS in the [official documentation](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### 3. Clone the Repository

```sh
git clone https://github.com/kyrylop/c-sql-db-cleanup.git
cd c-sql-db-cleanup
```

### 4. Create and Activate a Virtual Environment

```sh
python -m venv venv
# On Windows
venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate
```

### 5. Install Requirements

```sh
pip install -r requirements.txt
```

## Environment Settings

Create a `.env` file in the project root with the following variables:

```env
DB_HOST=your_db_host
DB_NAME=your_db_name
DB_PORT=your_db_port(1433 as usually for MS SQL database)
USERNAME=your_user_name
PASSWORD=your_user_password
DRIVER=SQL Server
START_DATE=date_to_which_clear_data (format YYYYMMDD)
DRY_RUN=True (True - for testing script, False - clear database)
```

Adjust the values according to your database configuration.

## Usage

Run your cleanup script as needed:

# C SQL DB Cleanup

A Python project for cleaning up your database.

## Getting Started

### 1. Install Python

Download and install Python 3.10 or newer from the [official Python website](https://www.python.org/downloads/).  
Make sure to check the box "Add Python to PATH" during installation.

### 2. Install SQL Server ODBC Driver

- **Windows:**  
  Download and install the [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).
- **macOS/Linux:**  
  Follow the instructions for your OS in the [official documentation](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### 3. Clone the Repository

```sh
git clone https://github.com/kyrylop/c-sql-db-cleanup.git
cd c-sql-db-cleanup
```

### 4. Create and Activate a Virtual Environment

```sh
python -m venv venv
# On Windows
venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate
```

### 5. Install Requirements

```sh
pip install -r requirements.txt
```

## Environment Settings

Create a `.env` file in the project root with the following variables:

```env
DB_HOST=your_db_host
DB_NAME=your_db_name
DB_PORT=your_db_port(1433 as usually for MS SQL database)
USERNAME=your_user_name
PASSWORD=your_user_password
DRIVER=SQL Server
START_DATE=date_to_which_clear_data (format YYYYMMDD)
DRY_RUN=True (True - for testing purposes, False - to persist changes in DB)
```

Adjust the values according to your database configuration.

## Usage

Run your cleanup script as needed:

```sh
python main.py
```
