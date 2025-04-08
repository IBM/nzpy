import sys

import nzpy

import pytest

# Check if running in Jython
if 'java' in sys.platform:
    from javax.net.ssl import TrustManager, X509TrustManager
    from jarray import array
    from javax.net.ssl import SSLContext

    class TrustAllX509TrustManager(X509TrustManager):
        '''Define a custom TrustManager which will blindly accept all
        certificates'''

        def checkClientTrusted(self, chain, auth):
            pass

        def checkServerTrusted(self, chain, auth):
            pass

        def getAcceptedIssuers(self):
            return None
    # Create a static reference to an SSLContext which will use
    # our custom TrustManager
    trust_managers = array([TrustAllX509TrustManager()], TrustManager)
    TRUST_ALL_CONTEXT = SSLContext.getInstance("SSL")
    TRUST_ALL_CONTEXT.init(None, trust_managers, None)
    # Keep a static reference to the JVM's default SSLContext for restoring
    # at a later time
    DEFAULT_CONTEXT = SSLContext.getDefault()


@pytest.fixture
def trust_all_certificates(request):
    '''Decorator function that will make it so the context of the decorated
    method will run with our TrustManager that accepts all certificates'''
    # Only do this if running under Jython
    is_java = 'java' in sys.platform

    if is_java:
        from javax.net.ssl import SSLContext
        SSLContext.setDefault(TRUST_ALL_CONTEXT)

    def fin():
        if is_java:
            SSLContext.setDefault(DEFAULT_CONTEXT)

    request.addfinalizer(fin)


def testSocketMissing():
    conn_params = {
        'unix_sock': "/file-does-not-exist",
        'user': "doesn't-matter",
        'database': "dummy_db"
        }

    with pytest.raises(nzpy.InterfaceError):
        nzpy.connect(**conn_params)


def testDatabaseMissing(db_kwargs):
    db_kwargs["database"] = "missing-db"
    with pytest.raises(nzpy.ProgrammingError):
        nzpy.connect(**db_kwargs)

# This requires a line in pg_hba.conf that requires md5 for the database
# nzpy_md5


def testMd5(db_kwargs):
    db_kwargs["database"] = "nzpy_md5"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(nzpy.ProgrammingError, match='handshake'):
        nzpy.connect(**db_kwargs)


@pytest.mark.usefixtures("trust_all_certificates")
def testSsl(db_kwargs):
    db_kwargs["ssl"] = True
    with nzpy.connect(**db_kwargs):
        pass


def testUnicodeDatabaseName(db_kwargs):
    db_kwargs["database"] = "nzpy_sn\uFF6Fw"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(nzpy.ProgrammingError, match='handshake'):
        nzpy.connect(**db_kwargs)


def testBytesPassword(con, db_kwargs):
    # Create user
    username = 'boltzmann'
    password = 'cha\uFF6Fs'
    with con.cursor() as cur:
        cur.execute(
            "create user " + username + " with password '" + password + "';")
        con.commit()

        db_kwargs['user'] = username
        db_kwargs['password'] = password.encode('utf8')
        db_kwargs['database'] = 'nzpy_md5'
        with pytest.raises(nzpy.ProgrammingError, match='handshake'):
            nzpy.connect(**db_kwargs)

        cur.execute("drop user " + username)
        con.commit()


# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram-sha-256

def test_scram_sha_256(db_kwargs):
    db_kwargs["database"] = "nzpy_scram_sha_256"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(nzpy.ProgrammingError, match='handshake'):
        nzpy.connect(**db_kwargs)


'''
def testNotify(con):
    assert list(con.notifications) == []
    with con.cursor() as cursor:
        cursor.execute("LISTEN test")
        cursor.execute("NOTIFY test")
        con.commit()
        cursor.execute("VALUES (1, 2), (3, 4), (5, 6)")
        assert len(con.notifications) == 1
        assert con.notifications[0][1] == "test"
# This requires a line in pg_hba.conf that requires gss for the database
# nzpy_gss
def testGss(db_kwargs):
    db_kwargs["database"] = "nzpy_gss"
    # Should raise an exception saying gss isn't supported
    with pytest.raises(
            nzpy.InterfaceError,
            match="Authentication method 7 not supported by nzpy."):
        nzpy.connect(**db_kwargs)
def testBytesDatabaseName(db_kwargs):
    """ Should only raise an exception saying db doesn't exist """
    db_kwargs["database"] = bytes("nzpy_sn\uFF6Fw", 'utf8')
    with pytest.raises(nzpy.ProgrammingError, match='handshake'):
        nzpy.connect(**db_kwargs)
def test_broken_pipe(con, db_kwargs):
    with nzpy.connect(**db_kwargs) as db1:
        with db1.cursor() as cur1, con.cursor() as cur2:
            cur1.execute("select pg_backend_pid()")
            pid1 = cur1.fetchone()[0]
            cur2.execute("select pg_terminate_backend(%s)", (pid1,))
            try:
                cur1.execute("select 1")
            except Exception as e:
                assert isinstance(e, (socket.error, struct.error))
def testApplicatioName(db_kwargs):
    app_name = 'my test application name'
    db_kwargs['application_name'] = app_name
    with nzpy.connect(**db_kwargs) as db:
        cur = db.cursor()
        cur.execute(
            'select application_name from pg_stat_activity '
            ' where pid = pg_backend_pid()')
        application_name = cur.fetchone()[0]
        assert application_name == app_name
'''
