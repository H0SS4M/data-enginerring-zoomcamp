# -*- coding: utf-8 -*-
"""Homework: data talks club  - data extraction and ingestion.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Te-AT0lfh0GpChg1Rbd0ByEKOHYtWXfm

# **Homework**: Data talks club data engineering zoomcamp Data loading workshop

Hello folks, let's practice what we learned - Loading data with the best practices of data engineering.

Here are the exercises we will do

# 1. Use a generator

Remember the concept of generator? Let's practice using them to futher our understanding of how they work.

Let's define a generator and then run it as practice.

**Answer the following questions:**

- **Question 1: What is the sum of the outputs of the generator for limit = 5?**
- **Question 2: What is the 13th number yielded**

I suggest practicing these questions without GPT as the purpose is to further your learning.
"""

def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)
s = 0
for sqrt_value in generator:
    s += sqrt_value
    print(sqrt_value)
s

"""# 2. Append a generator to a table with existing data


Below you have 2 generators. You will be tasked to load them to duckdb and answer some questions from the data

1. Load the first generator and calculate the sum of ages of all people. Make sure to only load it once.
2. Append the second generator to the same table as the first.
3. **After correctly appending the data, calculate the sum of all ages of people.**



"""

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)

# Commented out IPython magic to ensure Python compatibility.
# %%capture
# !pip install dlt[duckdb] # Install dlt with all the necessary DuckDB dependencies

import dlt

pipeline = dlt.pipeline(destination='duckdb', dataset_name='peoples')

pipeline.run(people_1(),
  table_name="people",
	write_disposition="replace")

pipeline.run(people_2(),
	table_name="people",
	write_disposition="append")

import duckdb

conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

ages = conn.sql("SELECT sum(age) FROM people").df()
display(ages)



"""# 3. Merge a generator

Re-use the generators from Exercise 2.

A table's primary key needs to be created from the start, so load your data to a new table with primary key ID.

Load your first generator first, and then load the second one with merge. Since they have overlapping IDs, some of the records from the first load should be replaced by the ones from the second load.

After loading, you should have a total of 8 records, and ID 3 should have age 33.

Question: **Calculate the sum of ages of all the people loaded as described above.**

# Solution: First make sure that the following modules are installed:
"""

# Commented out IPython magic to ensure Python compatibility.
# #Install the dependencies
# %%capture
# !pip install dlt[duckdb]

import dlt

pipeline = dlt.pipeline(destination='duckdb', dataset_name='peoples')

pipeline.run(people_1(),
  table_name="people_merged",
	write_disposition="replace")

pipeline.run(people_2(),
	table_name="people_merged",
	write_disposition="merge",
  primary_key="ID")

ages = conn.sql("SELECT sum(age) FROM people_merged").df()
display(ages)

"""Questions? difficulties? We are here to help.
- DTC data engineering course channel: https://datatalks-club.slack.com/archives/C01FABYF2RG
- dlt's DTC cohort channel: https://dlthub-community.slack.com/archives/C06GAEX2VNX
"""