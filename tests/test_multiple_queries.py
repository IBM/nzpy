import nzpy
import unittest
import random

class TestVectorDml(unittest.TestCase):
    connection = None

    def setUp(self):
        if self.connection is None:
            self.connection = nzpy.connect(user="user", unix_sock='unix_sock_file_dir', database='dbname', logOptions=nzpy.LogOptions.Disabled)

    def tearDown(self):
        self.connection.close()

    def test_simple_dml(self):
        '''Test various DML operations (INSERT, UPDATE, DELETE, SELECT).
        Using a non-VECTOR column as reference'''
        with self.connection.cursor() as cur:
            try:
                cur.execute("""
                drop table t1 if exists;
                create table t1 (v1 int);
                insert into t1 select 111 union select 222;
                select * from t1;

                alter table t1 ADD COLUMN v2 VARCHAR(50);
                update t1 set v2='aaa' where v1 =111;
                update t1 set v2='bbb' where v1 =222;
                select * from t1;

                drop table names if exists;
                create table names (id int, first_name varchar(10),last_name varchar(10));
                insert into names select 1, 'John', 'Doe' union select 2, 'Mark', 'Powell';
                select * from names;
                """)

                expected_result_set = [
                    ([111], [222]),
                    ([111, 'aaa'], [222, 'bbb']),
                    ([1, 'John', 'Doe'], [2, 'Mark', 'Powell'])
                    ]
                self.assertEqual(cur.fetchall(), expected_result_set)

            finally:
                pass
