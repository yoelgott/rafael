# Yoel's Rafael Test

**Follow the instructions bellow chronologically**

### Environment Setup

I used a conda environment for the project.
<br>
For setup env run the following commands in project dir:
<br>
`conda create -n yoel_test_rafael python=3.8.10`
<br>
`conda activate yoel_test_rafael`
<br>
`pip install -f requirements.txt`

### DB Setup

I chose to use **SQLite database** for convenience and simplicity
<br>
Create DB - `python setup/db_setup.py`
<br>
This creates a db file with _'ads' table_ in path - `db/rafael.db`
<br><br>
Example query over db (after db setup) - `python db/example_query_db.py`

### Sorting Programs

The 3 sorting programs with the different limitations are under folder: _sort_programs_
<br>
In order to run the programs, follow these commands:
<br>
Step 1 - `python sort_programs/step_1.py`
<br>
Step 2 - `python sort_programs/step_2.py`
<br>
Step 3 - `python sort_programs/step_3.py`

### Test MergeLists Class

I wrote a test for the MergeList class (in _utils.py_) under _tests_ folder
<br>
Run test - `python tests/test_k_merge.py`

### API

I implemented the optional api that exposes the "get-ads" method with flask under _api_ folder
<br>
Run the server - `python api/app.py`
<br><br>
Send requests to the server and see results (while the server is running) run the following command:
<br>
`python api/api_usage.py`