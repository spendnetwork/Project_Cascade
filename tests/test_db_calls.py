def isConnected(conn):
    if not conn or conn.closed == 1:
        return False
    return True

def test_checkDataExists():
    pass

def test_createRegistryDataSQLQuery():
    pass

def test_connection(connection):
    assert isConnected(connection)

def test_fetchData():
    pass

def test_removeTableDuplicates():
    pass

def test_addDataToTable():
    pass

