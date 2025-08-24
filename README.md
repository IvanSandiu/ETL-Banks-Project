# ETL Project: Largest Banks Market Capitalization  

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)  
![Pandas](https://img.shields.io/badge/Library-Pandas-orange)  
![SQLite](https://img.shields.io/badge/Database-SQLite-green)  
![ETL](https://img.shields.io/badge/Process-ETL-yellow)  

---

## 📖 Overview  

This project implements an **ETL (Extract, Transform, Load) pipeline** that processes data about the largest banks in the world by market capitalization.  

The pipeline:  
1. **Extracts** raw data from a Wikipedia snapshot.  
2. **Transforms** market capitalization from USD to other currencies (`GBP`, `EUR`, `INR`).  
3. **Loads** the final dataset into both a CSV file and a SQLite database.  
4. Runs a set of **sample SQL queries**.  

Logging is also implemented to track progress at every stage.  

---

## 📂 Project Structure  

| File / Folder        | Description |
|-----------------------|-------------|
| `banks_project.py`    | Main ETL pipeline script |
| `Largest_banks_data.csv` | Output dataset in CSV format |
| `Banks.db`            | SQLite database containing the table `Largest_banks` |
| `code_log.txt`        | Log file with ETL execution details |

---

## ⚙️ ETL Pipeline

**Extract**:
Scrapes the table of the largest banks from a Wikipedia snapshot using BeautifulSoup.

**Transform**:
Reads exchange rates from a CSV file and converts market capitalization values from USD to GBP, EUR, and INR.

**Load**:
- Saves results into a CSV file (Largest_banks_data.csv).
- Loads data into a SQLite database (Banks.db).

**SQL Queries**:
Example queries executed in the script:
- SELECT * FROM Largest_banks;
- SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
- SELECT Name FROM Largest_banks LIMIT 5;

## ⚙️ Requirements  

This project requires **Python 3.8+** and the following libraries:  

```bash
pip install pandas beautifulsoup4 requests numpy
```

## 🚀 How to Run  

Clone the repository or download the script, then execute:  

```bash
python banks_project.py
```

![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
