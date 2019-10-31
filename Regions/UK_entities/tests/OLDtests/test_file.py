# Test line 87 onwards in runfile.py is functional. Maybe do a separate test for each mini function to ensure the whole thing can run.
# If one thing doesn't work here and the file gets
# still created then the rest of the if statement is skipped i.e. leven_dist etc.



# def test_if_source_file_exists_movetoregistrydata():
#     # Test that if source adj file exists then skip to cleaning the registry data file
#     # Could expand to checking that if each file exists then it moves on to the next section without
#     # duplicating or re-doing the process on the file that already exists.
#     pass
#




# test that delete duplicates within database actually returns a unique database
# pass in test_clustered.csv to the database, run deduplication on the table, then assert that it matches the test_deduplicated_data.csv
# could also combine this with a test_upload properly. Maybe upload test_clustered.csv and then assert that the table in the database is exactly equal to the original test_clustered.csv file.
# OR can test the sql query is an insert function as per the test_insert() function in the core repo (test_database.py)
# def test_uploadtodb():