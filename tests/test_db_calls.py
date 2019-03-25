def isConnected(conn):
    if not conn or conn.closed == 1:
        return False
    return True


def test_connection(connection):
    assert isConnected(connection)