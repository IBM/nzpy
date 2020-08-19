import nzpy
from datetime import datetime as Datetime, timezone as Timezone
import pytest
from warnings import filterwarnings
import pdb

# Tests relating to the basic operation of the database driver, driven by the
# nzpy custom interface.

@pytest.fixture
def db_table(request, con):
    filterwarnings("ignore", "DB-API extension cursor.next()")
    filterwarnings("ignore", "DB-API extension cursor.__iter__()")
    con.paramstyle = 'qmark'
    with con.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE t1 (f1 int primary key, "
            "f2 bigint not null, f3 varchar(50) null) ")

    def fin():
        try:
            with con.cursor() as cursor:
                cursor.execute("drop table t1")
        except nzpy.ProgrammingError:
            pass

    request.addfinalizer(fin)
    return con

def test_parallel_queries(db_table):
    with db_table.cursor() as cursor:
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)", (1, 1, ''))
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)", (2, 10, ''))
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)", (3, 100, ''))
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)", (4, 1000, ''))
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)",
            (5, 10000, ''))
        with db_table.cursor() as c1, db_table.cursor() as c2:
            c1.execute("SELECT f1, f2, f3 FROM t1")
            for row in c1:
                f1, f2, f3 = row
                c2.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > ?", (f1,))
                for row in c2:
                    f1, f2, f3 = row

# Run a query on a table, alter the structure of the table, then run the
# original query again.


def test_alter(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("select * from t1")
        cursor.execute("alter table t1 drop column f3 RESTRICT")
        cursor.execute("select * from t1")


# Run a query on a table, drop then re-create the table, then run the
# original query again.

def test_create(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("select * from t1")
        cursor.execute("drop table t1")
        cursor.execute("create table t1 (f1 int primary key)")
        cursor.execute("select * from t1")

#TO DO: rowcount returns -1 for select statement for nzpy(limitation due to backend). 
#So in future this test case can be removed

def test_row_count(db_table):
    with db_table.cursor() as cursor:
        expected_count = 57
        cursor.executemany(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)",
            tuple((i, i, '') for i in range(expected_count)))

        # Check rowcount after executemany
        assert expected_count == cursor.rowcount
        cursor.execute("SELECT count(*) FROM t1")
        
        result = cursor.fetchall()[0]
        # Check row_count without doing any reading first...
        assert [expected_count] == result 

        # Check rowcount after reading some rows, make sure it still
        # works...
        for i in range(expected_count // 2):
            cursor.fetchone()
        assert [expected_count] == result

    with db_table.cursor() as cursor:
        # Restart the cursor, read a few rows, and then check rowcount
        # again...
        cursor.execute("SELECT count(*) FROM t1")
        result = cursor.fetchall()[0]
        for i in range(expected_count // 3):
            cursor.fetchone()
        assert [expected_count] == result

        # Should be -1 for a command with no results
        cursor.execute("DROP TABLE t1")
        assert -1 == cursor.rowcount

        # should not error out in fin() hence creating another t1 table
        cursor.execute("create table t1 (f1 int primary key)")

def test_row_count_update(db_table):
    with db_table.cursor() as cursor:
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)", (1, 1, ''))
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)", (2, 10, ''))
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)", (3, 100, ''))
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)", (4, 1000, ''))
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)",
            (5, 10000, ''))
        cursor.execute("UPDATE t1 SET f3 = ? WHERE f2 > 101", ("Hello!",))
        assert cursor.rowcount == 2


def test_unicode_query(cursor):
    cursor.execute(
        "CREATE TEMPORARY TABLE \u043c\u0435\u0441\u0442\u043e "
        "(\u0438\u043c\u044f VARCHAR(50), "
        "\u0430\u0434\u0440\u0435\u0441 VARCHAR(250))")


def test_executemany(db_table):
    with db_table.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)",
            ((1, 1, 'Avast ye!'), (2, 1, '')))

        cursor.executemany(
            "select ?",
            (
                 "2014-05-07 00:00:00",
                 "2014-05-07 00:00:00"))


# Check that autocommit stays off
# We keep track of whether we're in a transaction or not by using the
# READY_FOR_QUERY message.
def test_transactions(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("commit")
        db_table.autocommit = False
        cursor.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (?, ?, ?)",
            (1, 1, "Zombie"))
        cursor.execute("rollback")
        cursor.execute("select count(*) from t1")

        assert cursor.fetchone() == [0]

def test_context_manager_class(con):
    assert '__enter__' in nzpy.core.Cursor.__dict__
    assert '__exit__' in nzpy.core.Cursor.__dict__

    with con.cursor() as cursor:
        cursor.execute('select 1')

def test_database_error(cursor):
    with pytest.raises(nzpy.ProgrammingError):
        cursor.execute("INSERT INTO t99 VALUES (1, 2, 3)")

'''
# rolling back when not in a transaction doesn't generate a warning
def test_rollback_no_transaction(con):
    # Remove any existing notices
    con.notices.clear()
    with con.cursor() as cursor:

        # First, verify that a raw rollback does produce a notice
        con.execute(cursor, "rollback", None)

        pdb.set_trace()
        assert 1 == len(con.notices)

        # 25P01 is the code for no_active_sql_tronsaction. It has
        # a message and severity name, but those might be
        # localized/depend on the server version.
        assert con.notices.pop().get(b'C') == b'25P01'

        # Now going through the rollback method doesn't produce
        # any notices because it knows we're not in a transaction.
        con.rollback()

        assert 0 == len(con.notices)

def test_deallocate_prepared_statements(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("select * from t1")
        cursor.execute("alter table t1 drop column f3")
        cursor.execute("select count(*) from pg_prepared_statements")
        res = cursor.fetchall()
        assert res[0][0] == 1

def test_insert_returning(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("CREATE TABLE t2 (id serial, data text)")

        # Test INSERT ... RETURNING with one row...
        cursor.execute(
            "INSERT INTO t2 (data) VALUES (?) RETURNING id", ("test1",))
        row_id = cursor.fetchone()[0]
        cursor.execute("SELECT data FROM t2 WHERE id = ?", (row_id,))
        assert "test1" == cursor.fetchone()[0]

        assert cursor.rowcount == 1

        # Test with multiple rows...
        cursor.execute(
            "INSERT INTO t2 (data) VALUES (?), (?), (?) "
            "RETURNING id", ("test2", "test3", "test4"))
        assert cursor.rowcount == 3
        ids = tuple([x[0] for x in cursor])
        assert len(ids) == 3

def test_int_oid(cursor):
    # https://bugs.launchpad.net/nzpy/+bug/230796
    cursor.execute("SELECT typname FROM pg_type WHERE oid = ?", (100,))

def test_in(cursor):
    cursor.execute(
        "SELECT typname FROM pg_type WHERE oid = any(?)", ([16, 23],))
    ret = cursor.fetchall()
    assert ret[0][0] == 'bool'

def test_no_previous_tpc(con):
    con.tpc_begin('Stacey')
    with con.cursor() as cursor:
        cursor.execute("SELECT * FROM pg_type")
        con.tpc_commit()

#check that tpc_recover() doesn't start a transaction
def test_tpc_recover(con):
    con.tpc_recover()
    with con.cursor() as cursor:
        con.autocommit = True

        # If tpc_recover() has started a transaction, this will fail
        cursor.execute("VACUUM")

# An empty query should raise a ProgrammingError
def test_empty_query(cursor):
    with pytest.raises(nzpy.ProgrammingError):
        cursor.execute("")

'''
