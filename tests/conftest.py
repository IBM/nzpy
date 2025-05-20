import os
import subprocess
import sys
import nzpy
import pytest


@pytest.fixture(scope="class")
def db_kwargs():
    db_connect = {
        'user': os.environ.get('NZPY_USER'),
        'password': os.environ.get('NZPY_PASSWORD')
    }
    print(f'The value of user : {os.environ.get('NZPY_USER')}')
    print(f'The value of NZPY_PASSWORD : {os.environ.get('NZPY_PASSWORD')}')
    try:
        db_connect['port'] = 5480
    except KeyError:
        pass
    return db_connect


@pytest.fixture
def con(request, db_kwargs):
    if os.environ.get('NZPY_IS_REMOTE'):
        db_kwargs['host'] = os.environ.get('NZPY_HOST')
        db_kwargs['database'] = os.environ.get('NZPY_DATABASE')
    try:
        sql = ['''nzsql -d "system" -Axc "drop database nzpy_test" ''', ]
        newProc = subprocess.Popen(sql, stdout=subprocess.PIPE)
        newProc.wait()
    except Exception:
        pass  # consume exception is database does not exist
    try:
        sql = ['''nzsql -d "system" -Axc "create database nzpy_test" ''', ]
        newProc = subprocess.Popen(sql, stdout=subprocess.PIPE)
        newProc.wait()
    except Exception as exp:
        print(exp)
    print(f'The value of db_kwargs : {db_kwargs}')
    conn = nzpy.connect(**db_kwargs)
    print(f"The connection is created successfully")

    def fin():
        conn.rollback()
        try:
            conn.close()
        except nzpy.InterfaceError:
            pass

    request.addfinalizer(fin)
    return conn


@pytest.fixture
def cursor(request, con):
    cursor = con.cursor()

    def fin():
        cursor.close()

    request.addfinalizer(fin)
    return cursor


@pytest.fixture
def is_java():
    return 'java' in sys.platform.lower()
