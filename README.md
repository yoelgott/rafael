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
Example query (after db setup) - `'select * from ads'`