📊 Student ETL Pipeline (Python)

This is my **first Data Engineering project** built using **Python**. The goal of this project is to understand and implement a **basic ETL (Extract, Transform, Load) pipeline** using a real-world–style dataset.

🚀 Project Overview

In real data engineering work, raw data is rarely clean. This project demonstrates how to:

* Read raw student data from a CSV file
* Clean and transform the data
* Apply business rules (Pass / Fail logic)
* Load the cleaned data into a new CSV file

This project focuses on **clarity and fundamentals**, not advanced tools.


🧩 ETL Pipeline Breakdown

1️⃣ Extract

* Reads raw student data from `students_raw.csv`
* Uses Python file handling

2️⃣ Transform

* Removes header row
* Cleans whitespace in names
* Handles missing or invalid marks (`absent` or empty → `0`)
* Converts marks to integers
* Applies business rule:

  * Marks ≥ 40 → Pass
  * Marks < 40 → Fail
* Adds a new column `result`

3️⃣ Load

* Writes transformed data into `students_clean.csv`
* Saves the final structured dataset


📂 Project Structure


student-etl-pipeline/
│
├── students_raw.csv        # Raw input data
├── students_clean.csv      # Cleaned output data
├── etl_pipeline.py         # Python ETL script
├── README.md               # Project documentation
```

🛠 Technologies Used

* Python 3
* File Handling
* Basic Data Transformation Logic

(No external libraries used — pure Python)


🎯 What I Learned

* How ETL works in real-world data engineering
* Handling missing and dirty data
* Applying business logic during transformation
* Writing clean data to a new source
* Building confidence with Python fundamentals


🔮 Future Improvements

* Use `csv` or `pandas` library
* Add logging instead of print statements
* Convert ETL steps into separate functions
* Load data into a database


🙌 Author

**Mohamed Aslam**
Aspiring Data Engineer


⭐ If you are learning Data Engineering, feel free to fork this project and improve it!
