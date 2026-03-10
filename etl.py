#read student data from a CSV file
import csv
import logging


logging.basicConfig(filename="demo", level=logging.INFO)

def extract_student_data(student_file):

    with open(student_file, "r") as file:
        reader =csv.reader(file)
        
        data = list(reader)

    return data

raw_data = extract_student_data("students_raw.csv")
logging.info("Raw data extracted: %s", raw_data)
#remove header
records = raw_data[1:]
for r in records:
    logging.info("Processing record: %s", r)

#transform student data into a list of lists
rows = []

for r in records:
    columns = r
    columns[1] = columns[1].strip()
    if columns[3] == "" or columns[3] == "absent":
        columns[3] = "0"
    marks = int(columns[3])
    if marks < 40:
        result = "Fail"
    else:
        result = "Pass"
    columns.append(result)
    rows.append(columns)

logging.info("Transformed rows: %s", rows)
#load transformed data into a new CSV file
with open("students_clean.csv", "w") as file:
    file.write("student_id,name,department,marks,result\n")  # header

    for row in rows:
        line = ",".join(row)
        file.write(line + "\n")

logging.info("ETL pipeline completed. File saved as students_clean.csv")
