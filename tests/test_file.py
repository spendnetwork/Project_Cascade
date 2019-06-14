# Test line 87 onwards in runfile.py is functional. Maybe do a separate test for each mini function to ensure the whole thing can run.
# If one thing doesn't work here and the file gets
# still created then the rest of the if statement is skipped i.e. leven_dist etc.



# def test_raw_data_access():
#     # Test that either a raw data file exists or the connection can be made to the database
#     pass
#
# test imported data matches usecols format
#
# def test_if_source_file_exists_movetoregistrydata():
#     # Test that if source adj file exists then skip to cleaning the registry data file
#     # Could expand to checking that if each file exists then it moves on to the next section without
#     # duplicating or re-doing the process on the file that already exists.
#     pass
#
# def test_directory_creation():
#     #Tests if directories don't exist that they get created.

#
# def test_same_directories():
#     # Tests that

# Test that the data retrieved from the database consists of strings

# test upload data to db actually uploads it

# Use mocks to avoid having to make live-calls to a function that has an external function call inside it:
# https://realpython.com/python-cli-testing/#pytest
# def initial_transform(data):
#     outside_module.do_something()
#     return data



# test that delete duplicates within database actually returns a unique database
# pass in test_clustered.csv to the database, run deduplication on the table, then assert that it matches the test_deduplicated_data.csv
# could also combine this with a test_upload properly. Maybe upload test_clustered.csv and then assert that the table in the database is exactly equal to the original test_clustered.csv file.
# OR can test the sql query is an insert function as per the test_insert() function in the core repo (test_database.py)
# def test_uploadtodb():