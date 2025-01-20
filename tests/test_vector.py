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

    def test_vector_dml(self):
        '''Test various DML operations (INSERT, UPDATE, DELETE, SELECT).
        Using a non-VECTOR column as reference'''
        with self.connection.cursor() as cur:
            try:
                cur.execute("SET DATATYPE_BACKWARD_COMPATIBILITY ON")
                cur.execute("DROP TABLE vector_table IF EXISTS")
                cur.execute("CREATE TABLE vector_table(id INT, vector_col VECTOR)")

                cur.execute("INSERT INTO vector_table(id, vector_col) VALUES (1, '[1,2,3]')")
                cur.execute("INSERT INTO vector_table(id, vector_col) VALUES (2, '[4,5,6]')")
                cur.execute("INSERT INTO vector_table(id, vector_col) VALUES (3, '[7,8,9]')")
                cur.execute("SELECT * FROM vector_table ORDER BY id")

                expected_rows = (
                    [1, '[1,2,3]'],
                    [2, '[4,5,6]'],
                    [3, '[7,8,9]']
                )
                self.assertEqual(cur.fetchall(), expected_rows)

                cur.execute("SELECT * FROM vector_table WHERE id = 1")
                self.assertEqual(cur.fetchone(), [1, '[1,2,3]'])
                cur.execute("SELECT * FROM vector_table WHERE id = 2")
                self.assertEqual(cur.fetchone(), [2, '[4,5,6]'])
                cur.execute("SELECT * FROM vector_table WHERE id = 3")
                self.assertEqual(cur.fetchone(), [3, '[7,8,9]'])

                cur.execute("UPDATE vector_table SET vector_col = '[10,11,12]' WHERE id = 1")
                cur.execute("UPDATE vector_table SET vector_col = '[13,14,15]' WHERE id = 2")
                cur.execute("UPDATE vector_table SET vector_col = '[16,17,18]' WHERE id = 3")
                cur.execute("SELECT * FROM vector_table ORDER BY id")
                updated_rows = (
                    [1,'[10,11,12]'],
                    [2,'[13,14,15]'],
                    [3,'[16,17,18]']
                )
                self.assertEqual(cur.fetchall(), updated_rows)

                cur.execute("DELETE FROM vector_table WHERE id = 1")
                cur.execute("DELETE FROM vector_table WHERE id = 2")
                cur.execute("DELETE FROM vector_table WHERE id = 3")
                cur.execute("SELECT * FROM vector_table ORDER BY id")
                self.assertIsNone(cur.fetchone())

            finally:
                cur.execute("DROP TABLE vector_table")

    def test_vector_operators(self):
         with self.connection.cursor() as cur:
            try:
                cur.execute("SET DATATYPE_BACKWARD_COMPATIBILITY ON")
                cur.execute("DROP TABLE vector_table IF EXISTS")
                cur.execute("CREATE TABLE vector_table(id INT, embedding VECTOR)")

                cur.execute("INSERT INTO vector_table VALUES (1, '[1,2,3]')")
                cur.execute("INSERT INTO vector_table VALUES (2, '[4,5,6]')")
                cur.execute("SELECT * FROM vector_table ORDER BY id")
                expected_rows = (
                    [1, '[1,2,3]'],
                    [2, '[4,5,6]']
                )
                self.assertEqual(cur.fetchall(), expected_rows)

                cur.execute("SELECT embedding <-> '[3,4,5]' AS L2Distance FROM vector_table ORDER BY id")
                expected_rows = (
                    [3.4641016151377544],
                    [1.7320508075688772]
                )
                self.assertEqual(cur.fetchall(), expected_rows)

                cur.execute("SELECT embedding <+> '[3,4,5]' AS L1Distance FROM vector_table ORDER BY id")
                expected_rows = (
                    [6.0],
                    [3.0]
                )
                self.assertEqual(cur.fetchall(), expected_rows)

                cur.execute("SELECT embedding <=> '[3,4,5]' AS CosineDistance  FROM vector_table ORDER BY id")
                expected_rows = (
                    [0.017292370176009153],
                    [0.0007795246085285923]
                )
                self.assertEqual(cur.fetchall(), expected_rows)

                cur.execute("SELECT embedding <#> '[3,4,5]' AS InnerProduct FROM vector_table ORDER BY id")
                expected_rows = (
                    [-26],
                    [-62]
                )
                self.assertEqual(cur.fetchall(), expected_rows)

            finally:
                cur.execute("DROP TABLE vector_table")

    def test_vector_agg(self):
        d = {
            "SET DATATYPE_BACKWARD_COMPATIBILITY ON" : '',
            "DROP TABLE vector_table IF EXISTS" : '',
            "CREATE TABLE vector_table(id INT, vector_col VECTOR)" : '',
            "INSERT INTO vector_table VALUES(1, NULL)" : '',
            "INSERT INTO vector_table VALUES(1, '[1]')" : '',
            "INSERT INTO vector_table VALUES(2, '[2]')" : '',
            "INSERT INTO vector_table VALUES(3, '[3]')" : '',
            "INSERT INTO vector_table VALUES(4, '[1,2,3]')" : '',
            "INSERT INTO vector_table VALUES(5, '[4,5,6]')" : '',
            "INSERT INTO vector_table VALUES(6, '[7,8,9]')" : '',
            "SELECT * FROM vector_table ORDER BY vector_col" : "ERROR:  Unable to identify an operator '<' for types 'VECTOR' and 'VECTOR'\n\tYou will have to retype this query using an explicit cast\n\x00",
            "SELECT * FROM vector_table GROUP BY vector_col" : "ERROR:  Unable to identify an ordering operator '<' for type 'VECTOR'\n\tUse an explicit ordering operator or modify the query\n\x00",
            "SELECT FIRST_VALUE(id) OVER(ORDER BY vector_col) first_val FROM vector_table" : "ERROR:  Unable to identify an operator '<' for types 'VECTOR' and 'VECTOR'\n\tYou will have to retype this query using an explicit cast\n\x00",
            "SELECT id, FIRST_VALUE(vector_col) OVER(ORDER BY vector_col) first_val FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - FIRST_VALUE(VECTOR)\n\x00",
            "SELECT id, LAST_VALUE (vector_col) OVER(ORDER BY vector_col) last_val  FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - LAST_VALUE(VECTOR)\n\x00",
            "SELECT id, vector_col, RANK()       OVER(ORDER BY vector_col)      col_rank             FROM vector_table" : "ERROR:  Unable to identify an operator '<' for types 'VECTOR' and 'VECTOR'\n\tYou will have to retype this query using an explicit cast\n\x00",
            "SELECT id, vector_col, RANK()       OVER(ORDER BY vector_col DESC) col_rank_desc        FROM vector_table" : "ERROR:  Unable to identify an operator '>' for types 'VECTOR' and 'VECTOR'\n\tYou will have to retype this query using an explicit cast\n\x00",
            "SELECT id, vector_col, ROW_NUMBER() OVER(ORDER BY vector_col)      col_row_number       FROM vector_table" : "ERROR:  Unable to identify an operator '<' for types 'VECTOR' and 'VECTOR'\n\tYou will have to retype this query using an explicit cast\n\x00",
            "SELECT id, vector_col, ROW_NUMBER() OVER(ORDER BY vector_col DESC) col_row_number_desc  FROM vector_table" : "ERROR:  Unable to identify an operator '>' for types 'VECTOR' and 'VECTOR'\n\tYou will have to retype this query using an explicit cast\n\x00",
            "SELECT COUNT (vector_col)          FROM vector_table" : ([6],),
            "SELECT COUNT (DISTINCT vector_col) FROM vector_table" : "ERROR:  Unable to identify an equality operator for type 'VECTOR'\n\x00",
            "SELECT STDDEV     (vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - STDDEV(VECTOR)\n\x00",
            "SELECT STDDEV_POP (vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - STDDEV_POP(VECTOR)\n\x00",
            "SELECT STDDEV_SAMP(vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - STDDEV_SAMP(VECTOR)\n\x00",
            "SELECT VARIANCE   (vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - VARIANCE(VECTOR)\n\x00",
            "SELECT VAR_POP    (vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - VAR_POP(VECTOR)\n\x00",
            "SELECT VAR_SAMP   (vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - VAR_SAMP(VECTOR)\n\x00",
            "SELECT MIN        (vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - MIN(VECTOR)\n\x00",
            "SELECT MAX        (vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - MAX(VECTOR)\n\x00",
            "SELECT SUM        (vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - SUM(VECTOR)\n\x00",
            "SELECT MEDIAN     (vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - MEDIAN(VECTOR)\n\x00",
            "SELECT AVG        (vector_col) FROM vector_table" : "ERROR:  Unable to select an aggregate function from multiple potential matches - AVG(VECTOR)\n\x00",
            "SET DATATYPE_BACKWARD_COMPATIBILITY ON" : '',
            "DROP TABLE vector_table if exists" : '',
            "DROP TABLE vector_table_1 if exists" : '',
            "CREATE TABLE vector_table_1(id INT, vector_col VECTOR)" : '',
            "INSERT INTO vector_table_1(id, vector_col) VALUES (1, '[1,2,3]')" : '',
            "INSERT INTO vector_table_1(id, vector_col) VALUES (2, '[4,5,6]')" : '',
            "INSERT INTO vector_table_1(id, vector_col) VALUES (3, '[7,8,9]')" : '',
            "SELECT * FROM vector_table_1 ORDER BY id" : ([1, '[1,2,3]'], [2, '[4,5,6]'], [3, '[7,8,9]']),
            "DROP TABLE vector_table_1" : ''
        }
        with self.connection.cursor() as cur:
            try:
                for key,value in d.items():
                    try:
                        cur.execute(key)
                        expected_rows = (value)
                        r = cur
                        if value!='':
                            r=cur.fetchall()
                            print('The expected value: ',expected_rows)
                            print('The current value is : ',r)
                            self.assertEqual(r, expected_rows)
                    except nzpy.core.ProgrammingError as e:
                        expected_rows = value.strip()
                        self.assertEqual(str(e).strip(), expected_rows.strip())
            finally:
                pass
