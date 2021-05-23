# Yoel's Rafael Test

### Environment Setup

I used a conda environment for the project.
<br>
For setup env run the following command in project dir:
<br>
`conda create --name yoel_test_rafael --file requirements.txt`

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