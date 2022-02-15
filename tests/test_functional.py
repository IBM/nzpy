import unittest
import subprocess
import nzpy


class testnzpy(unittest.TestCase):

    def setUp(self):
        self.setupDb()
        self.conn = nzpy.connect(user="admin", password="password",
                                 host='localhost',
                                 port=5480, database="nzpy_test",
                                 securityLevel=0, logLevel=0)
        self.cursor = self.conn.cursor()

    # common methods for all objects
    @classmethod
    def executeSQL(cls, dbname, query):
        sql = ["nzsql", "-d", dbname, "-Axc", query]
        newProc = subprocess.Popen(sql, stdout=subprocess.PIPE)
        newProc.wait()
        if newProc.returncode != 0:
            raise Exception("Query execution failed:: " + str(dbname) +
                            " : " + str(query))
        return newProc.communicate()[0].decode()

    @classmethod
    def executeShellCmd(cls, cmd):
        newProc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        newProc.wait()
        if newProc.returncode != 0:
            raise Exception("Command execution failed:: " + str(cmd))
        print(str(cmd) + " COMPLETED")
        return newProc.communicate()[0].decode()

    @classmethod
    def setupDb(cls):
        try:
            cls.executeSQL("system", "drop database nzpy_test")
        except Exception:
            pass  # consume exception is database does not exist
        try:
            cls.executeSQL("system", "create database nzpy_test")
        except Exception as exp:
            print(exp)

    @classmethod
    def getRowCount(cls, output):
        return int(output.strip().split('|')[1])

    @classmethod
    def performOperation(cls, opName):
        try:
            with open(opName, "r") as fp:
                cmd = fp.readline()
                while cmd:
                    if cmd.split()[0] == "nzsql":
                        dbname = cmd.split()[2]
                        query = cmd.split('"')[1]
                        cls.executeSQL(dbname, query)
                    else:
                        cls.executeShellCmd(cmd.strip())
                    cmd = fp.readline()
        except Exception as exp:
            print(exp)

    def test_Update(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("UPDATE all_datatypes SET a_char20=?, "
                            "a_varchar1=?, "
                            "a_varchar50=?, a_real=?, a_double=?, "
                            "a_byteint=?, a_smallint=?, "
                            "a_int=?, a_bigint=?, a_numps=?, "
                            "a_nump=?, a_date=?, a_time=?, "
                            "a_timetz=?, a_timestamp=?, a_interval=?",
                            ('jeDHTrgMRvsNwftessEg', 'a',
                             'sdfsdrtdsrcsfsdfsd32423fd%$',
                             100, 6.6666, 12, 1111, 200, 111617881,
                             4456456.456367, 56123, '2001-01-01',
                             '12:12:12', '22:22:22+12:21',
                             '1223-12-12 12:12:12',
                             '22 years 22 months 22 days 22 hours 22 minutes '
                             '22 seconds'))
        self.assertEqual(12, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("UPDATE all_datatypes SET a_char20="
                            "'LaN$SCCTn%lWe"
                            "mRvVEBq',a_varchar1='X',a_varchar50="
                            "'aS53d13Dds3c',a_real= "
                            "53.442,a_double=-1234.3456423,a_byteint=-128,"
                            "a_smallint="
                            "32767,a_int=43523565,a_bigint=23145355656334534,"
                            "a_numps="
                            "34524123.456532,a_nump=12345,a_date='1999-09-25',"
                            "a_time='12:37:25',"
                            "a_timetz='20:15:35+5:30',a_timestamp = "
                            "'1999-09-25 12:37:25',"
                            "a_interval='15 years 13 months 25 days"
                            " 13:15:38'")
        self.assertEqual(12, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("UPDATE all_datatypes SET a_char5= null,a_char20= "
                            "'a+34%sdf$sga$',a_varchar1= '',a_varchar50 = ?,"
                            "a_real = -3445.23423"
                            ",a_double= null,a_byteint= 55,a_smallint  = ?,"
                            "a_bigint= 45575616"
                            "557835435,a_numps= null,a_nump= ?,a_date= null,"
                            "a_time= '23:"
                            "45:56',a_timetz= ?,a_timestamp= '2005-12-31 "
                            "12:45:34',"
                            "a_interval= null", ('DGdfg56h67+5$-jgs24'
                                                 'j89vfd%Q', 23111,
                                                 12321, '12:22:23+10:30'))
        self.assertEqual(12, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_Update1(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("UPDATE all_datatypes SET a_date=?,a_time= ?,"
                            "a_timetz= ?, a_timestamp= ?, a_interval= ? "
                            "WHERE a_char5= ? AND a_varchar1= ? AND "
                            "a_real= ? AND a_smallint= ? AND a_bigint= "
                            "? AND a_numps= ? AND a_char20 LIKE ? AND "
                            "a_varchar50 LIKE ?", ('1901-10-15', '15:46:50',
                                                   '13:12:25+08:21',
                                                   '1997-06-18 10:15:25',
                                                   '22 years 13 months '
                                                   '35 days 13 hours '
                                                   '90 minutes 23 seconds',
                                                   'pe*o', 'x',
                                                   -6.41581e-10, -31380,
                                                   780406997461985024,
                                                   53.00000000000000,
                                                   'r8EQa6I/eP3l:2%',
                                                   'g.4+:_*/Y-066VKPLs'
                                                   'gQxnQ/f5'))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_BulkDelete(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("DELETE FROM all_datatypes WHERE a_char20 "
                            "LIKE ?", ('trial%',))
        self.assertEqual(2, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_Bulkdelete(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("DELETE FROM all_datatypes WHERE a_char5 "
                            "LIKE ? || "
                            "'%' AND a_varchar1 LIKE ?  AND a_srno = "
                            "8346 AND a_smallint = ?", ('S', 'c', -29092))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_InsertNull(self):
        self.cursor.execute("CREATE TABLE t1 (c1 float4, c2 double, c3 int1, "
                            "c4 int2, c5 int4, c6 int8, c7 char(5), "
                            "c8 varchar(10), c9 nchar(50), "
                            "c10 nvarchar(120),c11 varbinary(12), "
                            "c12 ST_GEOMETRY(12), DATE_PROD DATE, "
                            "TIME_PROD TIME, INTERVAL_PROD INTERVAL,"
                            "TIMESTMP TIMESTAMP,TIMETZ_PROD "
                            "TIME WITH TIME ZONE, "
                            "c18 bool)")
        self.cursor.execute("INSERT INTO t1 VALUES(?,?,?,?,?,?,?,?,"
                            "?,?,?,?,?,?,?,?,?,?)", (None, None, None,
                                                     None, None, None,
                                                     None, None, None,
                                                     None, None, None,
                                                     None, None, None,
                                                     None, None, None,))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("SELECT * FROM t1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, \
            c13, c14, c15, c16, c17, c18 \
                in results:
            self.assertEqual(None, c1, "ERROR: Data Difference")
            self.assertEqual(None, c2, "ERROR: Data Difference")
            self.assertEqual(None, c3, "ERROR: Data Difference")
            self.assertEqual(None, c4, "ERROR: Data Difference")
            self.assertEqual(None, c5, "ERROR: Data Difference")
            self.assertEqual(None, c6, "ERROR: Data Difference")
            self.assertEqual(None, c7, "ERROR: Data Difference")
            self.assertEqual(None, c8, "ERROR: Data Difference")
            self.assertEqual(None, c9, "ERROR: Data Difference")
            self.assertEqual(None, c10, "ERROR: Data Difference")
            self.assertEqual(None, c11, "ERROR: Data Difference")
            self.assertEqual(None, c12, "ERROR: Data Difference")
            self.assertEqual(None, c13, "ERROR: Data Difference")
            self.assertEqual(None, c14, "ERROR: Data Difference")
            self.assertEqual(None, c15, "ERROR: Data Difference")
            self.assertEqual(None, c16, "ERROR: Data Difference")
            self.assertEqual(None, c17, "ERROR: Data Difference")
            self.assertEqual(None, c18, "ERROR: Data Difference")

        self.cursor.execute("drop table t1 ")

    def test_InsertNullDate(self):
        self.cursor.execute("CREATE TABLE \"Mixed Case Crap Table\" "
                            "(\"a Mixed Case Column\" int, ts TIMESTAMP, "
                            "dt date, tm time, ch varchar(100))")
        self.cursor.execute("INSERT INTO \"Mixed Case "
                            "Crap Table\" (dt) VALUES(?)", (None,))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("SELECT * FROM \"Mixed Case Crap Table\" ")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5 in results:
            self.assertEqual(None, c1, "ERROR: Data Difference")
            self.assertEqual(None, c2, "ERROR: Data Difference")
            self.assertEqual(None, c3, "ERROR: Data Difference")
            self.assertEqual(None, c4, "ERROR: Data Difference")
            self.assertEqual(None, c5, "ERROR: Data Difference")

        self.cursor.execute("drop table \"Mixed Case Crap Table\" ")

    def test_InsertNumeric(self):
        self.cursor.execute("CREATE TABLE all_numeric (col1 numeric (36,14),"
                            "col2 numeric (38,0),col3 numeric (38,38),"
                            "col4 numeric (3,1),col5 numeric (18,10),"
                            "col6 numeric (38,1))")
        self.cursor.execute("INSERT INTO all_numeric values "
                            "(111111111111111111111.3333333333333,"
                            "2222222222222222222222222222222222222,"
                            "0.9999999999999999999999999999999999999,"
                            ".1,1111111.1111,"
                            "111111111111111111111111111111111111.1)")
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_numeric ")

    def test_MixedCaseInsert(self):
        self.cursor.execute("CREATE TABLE \"Mixed Case Crap Table\" (\"a "
                            "Mixed Case Column\" "
                            "int, ts TIMESTAMP, dt date, "
                            "tm time, ch varchar(100))")
        self.cursor.execute("INSERT INTO \"Mixed Case Crap Table\" "
                            "VALUES (?,?,?,?,?)", (1000000000, '1979-9-6',
                                                   '1979-9-6 10:10:10.123456',
                                                   '10:10:10', 'a\\nbc\\tdde'
                                                               '\\\\fgh\\bsi'
                                                               'jk\\rl'))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table \"Mixed Case Crap Table\" ")

    def test_Insert(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("INSERT INTO all_datatypes (a_srno, a_char5, "
                            "a_char20, a_char1000, a_real,a_double, "
                            "a_byteint, a_smallint, a_int, a_bigint,"
                            "a_numps, a_nump, a_dec, a_angle) VALUES "
                            "(2,'UR', 'q.+nZ', '',?, ?, ?,"
                            " ?, ?, ?, ?, ?, ?, ?)", (-4.19711e-35, 7.771,
                                                      -115, -24930,
                                                      869594835,
                                                      -16484452487417700,
                                                      3331.00000000000000,
                                                      9916, 3342.0000000,
                                                      9))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_AlterTable1(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("ALTER TABLE all_datatypes drop COLUMN "
                            "a_char5 cascade")
        self.cursor.execute("ALTER TABLE all_datatypes  ADD COLUMN "
                            "a_char5 char(5)")
        self.cursor.execute("INSERT INTO all_datatypes (a_srno, a_char5, "
                            "a_char20,a_char1000, "
                            "a_varchar1, a_varchar50) VALUES (1,'abcde',"
                            "'abcdefghijklmn', 'abcdefghijklmnop', "
                            "'a','abcd12356khmf')")
        self.cursor.execute("UPDATE all_datatypes  SET a_char5 = 'aaaaa'")
        self.assertEqual(13, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_DirectAggr(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT max (a_srno), max (a_char5), "
                            "max (a_char20), max (a_char1000), "
                            "max (a_varchar1), max (a_varchar50), "
                            "max (a_real), max (a_byteint), "
                            "max (a_smallint), max (a_bigint),"
                            "max (a_dec), max (a_angle), "
                            "max (a_date), max (a_time), "
                            "max(a_timetz), max (a_timestamp),"
                            "max(a_interval)"
                            " FROM all_datatypes ")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, \
            c11, c12, c13, c14, c15, c16, c17 \
                in results:
            self.assertEqual(8346, c1, "ERROR: Data Difference")
            self.assertEqual('xSgB ', c2, "ERROR: Data Difference")
            self.assertEqual('zNuB', c3, "ERROR: Data Difference")
            self.assertEqual('y', c5, "ERROR: Data Difference")
            self.assertEqual('25 years 67 days 18:10:05', c17,
                             "ERROR: Data Difference")
            self.assertEqual(907713502163946368, c10, "ERROR: Data Difference")
            self.assertEqual('2197-02-15', c13, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_RemoteUnloadBasic(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("CREATE EXTERNAL TABLE "
                            "all_datatypes_retunload_ext "
                            "(a_srno integer ,a_char5 char(5), "
                            "a_char20 char(20) "
                            "not NULL, a_char1000 char(1000), "
                            "a_varchar1 varchar(1), "
                            "a_varchar50 varchar(50), "
                            "a_real float(5), "
                            "a_double float(15), "
                            "a_byteint byteint, "
                            "a_smallint smallint, "
                            "a_int integer, "
                            "a_bigint bigint, "
                            "a_numps numeric(36,14), "
                            "a_nump numeric(5), "
                            "a_dec decimal(15,7), a_angle integer, "
                            "a_date date, "
                            "a_time time, a_timetz timetz, "
                            "a_timestamp timestamp, "
                            "a_interval interval ) USING "
                            "(DATAOBJECT "
                            "('/tmp/all_datatypes_retunload_ext_uncompressed')"
                            "REMOTESOURCE 'python' "
                            "DATESTYLE 'DMY' DELIMITER ',' "
                            "COMPRESS 'false' ENCODING 'internal')")
        self.cursor.execute("INSERT INTO all_datatypes_retunload_ext "
                            "SELECT * from all_datatypes")
        self.assertEqual(12, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes_retunload_ext")
        self.cursor.execute("drop table all_datatypes")

    def test_RemoteUnloadWithDelim(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("CREATE EXTERNAL TABLE "
                            "all_datatypes_retunload_ext "
                            "(a_srno integer ,a_char5  "
                            "char(5), a_char20 char(20) not NULL, "
                            "a_char1000 char(1000), a_varchar1 varchar(1), "
                            "a_varchar50 varchar(50), a_real float(5), "
                            "a_double float(15), a_byteint byteint, "
                            "a_smallint smallint, a_int integer, "
                            "a_bigint bigint, a_numps numeric(36,14), "
                            "a_nump numeric(5), a_dec decimal(15,7), "
                            "a_angle integer, a_date date, a_time time, "
                            "a_timetz timetz, a_timestamp "
                            "timestamp, a_interval interval ) "
                            "USING "
                            "(DATAOBJECT "
                            "('/tmp/all_datatypes_retunload_"
                            "ext_delim_default') "
                            "REMOTESOURCE "
                            "'python' DATESTYLE 'DMY' COMPRESS 'false' "
                            "ENCODING 'internal')")
        self.cursor.execute("INSERT INTO all_datatypes_retunload_ext "
                            "SELECT * from all_datatypes")
        self.assertEqual(12, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes_retunload_ext")
        self.cursor.execute("drop table all_datatypes")

    def test_RemoteUnloadFileSpec(self):
        self.performOperation("./setup_all_datatype")
        try:
            self.cursor.execute("CREATE EXTERNAL TABLE "
                                "all_datatypes_retunload_ext "
                                "(a_srno integer ,a_char5  char(5), "
                                "a_char20 char(20) not NULL, "
                                "a_char1000 char(1000), "
                                "a_varchar1 varchar(1), "
                                "a_varchar50 varchar(50), "
                                "a_real float(5), a_double float(15), "
                                "a_byteint byteint, a_smallint "
                                "smallint, a_int integer, a_bigint bigint, "
                                "a_numps numeric(36,14), "
                                "a_nump numeric(5), a_dec decimal(15,7), "
                                "a_angle integer, a_date date, "
                                "a_time time, a_timetz timetz, a_timestamp "
                                "timestamp, a_interval interval ) "
                                "USING (DATAOBJECT "
                                "('/tmp/all_datatypes_retunload_ext_rs_odbc') "
                                "REMOTESOURCE "
                                "'odbc' DELIMITER ',' DATESTYLE 'DMY' "
                                "COMPRESS 'false' ENCODING 'internal')")
            self.cursor.execute("INSERT INTO all_datatypes_retunload_ext "
                                "SELECT * from all_datatypes")
        except Exception as e:
            self.assertEqual(str(e), "ERROR:  Remotesource option "
                                     "of external table "
                                     "was not defined to load/unload "
                                     "using a python client\n\x00",
                             "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes_retunload_ext")
        self.cursor.execute("drop table all_datatypes")

    def test_RemoteUnloadBasicCompressed(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("CREATE EXTERNAL TABLE "
                            "all_datatypes_retunload_ext "
                            "(a_srno integer, a_char5"
                            "char(5), a_char20 char(20) not NULL, "
                            "a_char1000 char(1000), "
                            "a_varchar1 varchar(1), a_varchar50 varchar(50), "
                            "a_real float(5), "
                            "a_double float(15), a_byteint byteint, "
                            "a_smallint smallint, "
                            "a_int integer, a_bigint bigint, "
                            "a_numps numeric(36,14), "
                            "a_nump numeric(5), a_dec decimal(15,7), "
                            "a_angle integer, "
                            "a_date date, a_time time, a_timetz timetz, "
                            "a_timestamp timestamp, "
                            "a_interval interval ) USING "
                            "(DATAOBJECT "
                            "('/tmp/all_datatypes_retunload_ext_compressed')"
                            "REMOTESOURCE 'python' COMPRESS 'true' FORMAT "
                            "'internal' ENCODING 'internal')")
        self.cursor.execute("INSERT INTO all_datatypes_retunload_ext "
                            "SELECT * from all_datatypes")
        self.assertEqual(12, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes_retunload_ext")
        self.cursor.execute("drop table all_datatypes")

    def test_Joins1(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT * FROM all_datatypes AS tab1 NATURAL JOIN "
                            "all_datatypes AS tab2 ORDER BY "
                            "tab1.a_srno, tab2.a_srno")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, \
            c12, c13, c14, c15, c16, c17, c18, c19, c20, c21 \
                in results:
            c = c + 1
        self.assertEqual(5, c, "ERROR: Data Difference")
        self.cursor.execute("SELECT * FROM all_datatypes AS tab1 INNER JOIN "
                            "all_datatypes AS tab2 using(a_srno) ORDER BY "
                            "tab1.a_srno, tab2.a_srno")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, \
            c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, \
            c21, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, \
            d12, d13, d14, d15, d16, d17, d18, d19, d20 \
                in results:
            c = c + 1
        self.assertEqual(12, c, "ERROR: Data Difference")
        self.cursor.execute("SELECT * FROM all_datatypes SELF JOIN "
                            "all_datatypes USING (a_srno)")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, \
            c13, c14, c15, c16, c17, c18, c19, c20, c21, d1, d2, \
            d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15, \
            d16, d17, d18, d19, d20 \
                in results:
            c = c + 1
        self.assertEqual(12, c, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_Joins2(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT * FROM all_datatypes AS tab1 "
                            "RIGHT OUTER JOIN "
                            "all_datatypes AS tab2 USING(a_char20) ORDER BY "
                            "tab1.a_srno,tab2.a_srno")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, \
            c11, c12, c13, c14, c15, c16, c17, c18, c19, \
            c20, c21, d1, d2, d3, d4, d5, d6, d7, d8, d9, \
            d10, d11, d12, d13, d14, d15, d16, d17, d18, d19, d20 \
                in results:
            c = c + 1
        self.assertEqual(14, c, "ERROR: Data Difference")
        self.cursor.execute("SELECT * FROM all_datatypes AS tab1 LEFT OUTER "
                            "JOIN all_datatypes AS tab2 USING(a_char20) "
                            "ORDER BY tab1.a_srno,tab2.a_srno")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, \
            c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, \
            d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, \
            d14, d15, d16, d17, d18, d19, d20 \
                in results:
            c = c + 1
        self.assertEqual(14, c, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_Joins3(self):
        self.performOperation("./setup_all_datatype")
        self.performOperation("./setup_all_latin")
        self.cursor.execute("SELECT * FROM all_Latin_datatypes, all_datatypes "
                            "ORDER BY all_latin_datatypes.a_srno,"
                            "all_datatypes.a_srno")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2, c3, c4, c5, c6, c7, c8, c9, c10,\
            c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, \
            c21, d1, d2, d3, d4, d5 \
                in results:
            c = c + 1
        self.assertEqual(180, c, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")
        self.cursor.execute("drop table all_latin_datatypes")

    def test_DirectMixedComments(self):
        self.cursor.execute("CREATE TABLE test (int1 int, char1 char)")
        self.cursor.execute("INSERT INTO test VALUES /* "
                            "this is a comment */ (1, 'c')")
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("INSERT INTO test VALUES -- "
                            "this is a comment \n (2, 'a')")
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("INSERT INTO test VALUES /* this is "
                            "-- a comment */ (3, 'b')")
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table test ")

    def test_NumericFnEscSeq(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT abs(a_int), abs(a_smallint), "
                            "abs (a_bigint), "
                            "abs(a_double), abs(a_numps) FROM all_datatypes "
                            "ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5 in results:
            self.assertEqual(798339977, c1, "ERROR: Data Difference")
            self.assertEqual(31380, c2, "ERROR: Data Difference")
            self.assertEqual(780406997461985024, c3, "ERROR: Data Difference")
            self.assertEqual(4242424422.60277, c4, "ERROR: Data Difference")
            self.assertEqual('53.00000000000000', c5, "ERROR: Data Difference")
        self.cursor.execute("SELECT round(a_numps), round(a_real), "
                            "round(a_double) "
                            "FROM all_datatypes ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3 in results:
            self.assertEqual('53', c1, "ERROR: Data Difference")
            self.assertEqual(0, c2, "ERROR: Data Difference")
            self.assertEqual(-4242424423, c3, "ERROR: Data Difference")

        self.cursor.execute("SELECT sqrt(a_srno), sqrt(a_nump) "
                            "FROM all_datatypes "
                            "ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2 in results:
            self.assertEqual(7.615773105863909, c1, "ERROR: Data Difference")
            self.assertEqual(None, c2, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_SetOperator1(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT a_srno, a_char5, a_varchar1 "
                            "FROM all_datatypes "
                            "WHERE a_srno = ANY (SELECT a_srno "
                            "FROM all_datatypes "
                            "WHERE a_char1000 LIKE  ?) ORDER "
                            "BY a_srno", ('%',))
        results = self.cursor.fetchall()
        c = 0
        for c1, c2, c3 in results:
            c = c + 1
        self.assertEqual(12, c, "ERROR: Data Difference")

        self.cursor.execute("SELECT a_srno, a_char5, a_date FROM "
                            "all_datatypes "
                            "WHERE a_srno = SOME (SELECT a_srno "
                            "FROM all_datatypes "
                            "WHERE a_date LIKE  ?) ORDER BY a_srno", ('%',))
        results = self.cursor.fetchall()
        c = 0
        for c1, c2, c3 in results:
            c = c + 1
        self.assertEqual(11, c, "ERROR: Data Difference")

        self.cursor.execute("SELECT a_srno,a_varchar50 "
                            "FROM all_datatypes "
                            "a WHERE a_srno > ALL "
                            "(SELECT a_srno FROM all_datatypes "
                            "b WHERE a.a_srno <> b.a_srno)")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2 in results:
            c = c + 1
        self.assertEqual(1, c, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_SetOperator2(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT a_srno, a_char20 FROM "
                            "all_datatypes WHERE EXISTS "
                            "(SELECT a_srno FROM all_datatypes "
                            "WHERE a_char5 LIKE ?) "
                            "ORDER BY a_srno LIMIT 5", ('%',))
        results = self.cursor.fetchall()
        c = 0
        for c1, c2 in results:
            c = c + 1
        self.assertEqual(5, c, "ERROR: Data Difference")
        self.cursor.execute("SELECT a_srno,a_char20, a_interval "
                            "FROM all_datatypes "
                            "WHERE NOT EXISTS (SELECT a_char5 FROM "
                            "all_datatypes WHERE a_interval LIKE  ?) "
                            "ORDER BY a_srno DESC LIMIT 5",
                            ('25 years 67 days 18 hours 10 minutes',))
        results = self.cursor.fetchall()
        c = 0
        for c1, c2, c3 in results:
            c = c + 1
        self.assertEqual(5, c, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_SetOperator3(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT a_srno, a_char20 FROM all_datatypes "
                            "WHERE a_char20 NOT IN (a_char5, "
                            "a_varchar50 )ORDER BY a_srno")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2 in results:
            c = c + 1
        self.assertEqual(11, c, "ERROR: Data Difference")
        self.cursor.execute("SELECT a_srno, a_time FROM all_datatypes a WHERE "
                            "a_srno IN (SELECT a_srno FROM all_datatypes b "
                            "WHERE a.a_srno = b.a_srno) ORDER BY a_srno")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2 in results:
            c = c + 1
        self.assertEqual(12, c, "ERROR: Data Difference")
        self.cursor.execute("SELECT a_srno, a_nump FROM "
                            "all_datatypes WHERE a_char5 "
                            "NOT IN (a_varchar1) AND a_srno "
                            "IN (SELECT a_srno FROM "
                            "all_datatypes WHERE a_nump  IS "
                            "NOT NULL) ORDER BY a_srno")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2 in results:
            c = c + 1
        self.assertEqual(9, c, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_SetOperator4(self):
        self.performOperation("./setup_all_latin")
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT a_srno, a_char20, "
                            "a_char1000 FROM all_datatypes "
                            "UNION SELECT a_srno, a_char10, "
                            "a_char1000 FROM all_latin_datatypes "
                            "WHERE a_srno IN (SELECT a_srno "
                            "FROM all_datatypes WHERE a_nump "
                            "IS NOT NULL)"
                            " ORDER BY a_srno")
        results = self.cursor.fetchall()
        c = 0
        for c1, c2, c3 in results:
            c = c + 1
        self.assertEqual(12, c, "ERROR: Data Difference")
        self.cursor.execute("SELECT * FROM ((SELECT a_char20 from "
                            "all_datatypes) "
                            "UNION ALL (SELECT a_char10 FROM "
                            "all_latin_datatypes)) AS a "
                            "ORDER BY 1")
        results = self.cursor.fetchall()
        expected = [[None], ['9'], ['BEEQxEnCuu-'], ['H5'],
                    ['HWFowZoZjwoXAj'], ['UZ3u2BocpErtN'],
                    ['gnXvyL'], ['q.+nZ'], ['r8EQa6I/eP3l:2RGR3tC'],
                    ['rbIx6*y'], ['trial'],
                    ['trial'], ['zNuB'], ['¢'], ['\xad'], ['·'], ['·ÃõýÉ'],
                    ['ÁãÑê'], ['ÄÖì'],
                    ['ÄÛºà'], ['Í»ü±'], ['ÕâÓÆ²ù¥'], ['ÖÔø'], ['Û\xadÃÐé°Ô'],
                    ['à'], ['àë'], ['òÛì']]
        output = []
        for c1 in results:
            output.append(c1)
        self.assertEqual(expected, output, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")
        self.cursor.execute("drop table ALL_LATIN_DATATYPES")

    def test_ExecuteScalar(self):
        self.performOperation("./setup_all_latin")
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT a_srno, a_char20, (SELECT a_char10 FROM "
                            "all_latin_datatypes WHERE "
                            "all_latin_datatypes.a_srno = 1) "
                            "FROM all_datatypes ORDER BY a_srno")
        results = self.cursor.fetchall()
        expected = [58, 'r8EQa6I/eP3l:2RGR3tC', 258, 'trial', 651,
                    'rbIx6*y', 698, 'BEEQxEnCuu-', 1628,
                    '9', 3336, 'q.+nZ', 5631, 'gnXvyL', 6324,
                    'UZ3u2BocpErtN', 6419, 'HWFowZoZjwoXAj',
                    6875, 'zNuB', 7253, 'H5', 8346, 'trial']
        output = []
        for c1, c2, c3 in results:
            output.append(c1)
            output.append(c2)
        self.assertEqual(expected, output, "ERROR: Data Difference")

        self.cursor.execute("SELECT a_srno, a_char20 FROM all_datatypes "
                            "WHERE 1 = (SELECT a_srno FROM "
                            "all_latin_datatypes "
                            "WHERE a_srno = 1)ORDER BY a_srno")
        results = self.cursor.fetchall()
        output = []
        for c1, c2 in results:
            output.append(c1)
            output.append(c2)
        self.assertEqual(expected, output, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")
        self.cursor.execute("drop table ALL_LATIN_DATATYPES")

    def test_FloatInIntCol(self):
        self.cursor.execute("CREATE TABLE test (col1 int)")
        self.cursor.execute("INSERT INTO test VALUES (.99)")
        self.cursor.execute("SELECT * from test")
        results = self.cursor.fetchall()
        for c in results:
            self.assertEqual([1], c, "ERROR: Data Difference")
        self.cursor.execute("drop table test")

    def test_FnEscSeq(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT ascii(a_srno), ascii(a_char20), "
                            "ascii(a_numps), "
                            "ascii(a_date), ascii(a_timestamp) "
                            "FROM all_datatypes "
                            "ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5 in results:
            self.assertEqual(53, c1, "ERROR: Data Difference")
            self.assertEqual(114, c2, "ERROR: Data Difference")
            self.assertEqual(53, c3, "ERROR: Data Difference")
            self.assertEqual(49, c4, "ERROR: Data Difference")
            self.assertEqual(49, c5, "ERROR: Data Difference")

        self.cursor.execute("SELECT position(1 IN a_srno), "
                            "position(1 IN a_time), "
                            "position(1 IN a_interval) FROM all_datatypes "
                            "ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3 in results:
            self.assertEqual(0, c1, "ERROR: Data Difference")
            self.assertEqual(1, c2, "ERROR: Data Difference")
            self.assertEqual(24, c3, "ERROR: Data Difference")

        self.cursor.execute("SELECT repeat(a_srno, 2), repeat(a_date, -1), "
                            "repeat(a_numps, 3), "
                            "repeat(a_char5, 2) FROM all_datatypes "
                            "ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual('5858', c1, "ERROR: Data Difference")
            self.assertEqual('', c2, "ERROR: Data Difference")
            self.assertEqual('53.0000000000000053.0000000000000053.'
                             '00000000000000', c3, "ERROR: Data Difference")
            self.assertEqual('pe*o pe*o ', c4, "ERROR: Data Difference")

        self.cursor.execute("SELECT substring(a_char20, 2, 10), "
                            "substring(a_numps, 2, 10), "
                            "substring(a_date, 2, 15), substring"
                            "(a_varchar1, 1, 15) "
                            "FROM all_datatypes ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual('8EQa6I/eP3', c1, "ERROR: Data Difference")
            self.assertEqual('3.00000000', c2, "ERROR: Data Difference")
            self.assertEqual('982-04-22', c3, "ERROR: Data Difference")
            self.assertEqual('x', c4, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_ForCast1(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT a_srno,CAST (a_date as timestamp),"
                            "CAST (a_date as char(20)),"
                            "CAST (a_date as varchar(20))FROM all_datatypes "
                            "ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual(58, c1, "ERROR: Data Difference")
            self.assertEqual('1982-04-22 00:00:00.000000', c2,
                             "ERROR: Data Difference")
            self.assertEqual('1982-04-22', c3, "ERROR: Data Difference")
            self.assertEqual('1982-04-22', c4, "ERROR: Data Difference")

        self.cursor.execute("SELECT a_srno,CAST (a_time as interval),CAST "
                            "(a_time as char(20)),"
                            "CAST (a_time as varchar(20))FROM all_datatypes "
                            "ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual(58, c1, "ERROR: Data Difference")
            self.assertEqual('13:20:05', c2, "ERROR: Data Difference")
            self.assertEqual('13:20:05', c3, "ERROR: Data Difference")
            self.assertEqual('13:20:05', c4, "ERROR: Data Difference")

        self.cursor.execute("SELECT a_srno,CAST (a_interval as time), "
                            "CAST (a_interval as char(50)),"
                            "CAST (a_interval as varchar(50))FROM "
                            "all_datatypes "
                            "ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual(58, c1, "ERROR: Data Difference")
            self.assertEqual('00:05:10', c2, "ERROR: Data Difference")
            self.assertEqual('20 years 67 days 00:05:10', c3,
                             "ERROR: Data Difference")
            self.assertEqual('20 years 67 days 00:05:10', c4,
                             "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_ForCast2(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT a_srno,CAST (a_char20 as char(2)),"
                            "CAST (a_char20 as varchar(20)),"
                            "CAST (a_varchar50 as char(50)),CAST "
                            "(a_varchar50 as char(5)) "
                            "FROM all_datatypes ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5 in results:
            self.assertEqual(58, c1, "ERROR: Data Difference")
            self.assertEqual('r8', c2, "ERROR: Data Difference")
            self.assertEqual('r8EQa6I/eP3l:2RGR3tC', c3,
                             "ERROR: Data Difference")
            self.assertEqual('g.4+:_*/Y-066VKPLsgQxnQ/f5', c4,
                             "ERROR: Data Difference")
            self.assertEqual('g.4+:', c5, "ERROR: Data Difference")

        self.cursor.execute("SELECT a_srno,CAST (a_real as double),"
                            "CAST (a_real as char (30)),"
                            "CAST (a_real as varchar (30))FROM all_datatypes "
                            "ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual(58, c1, "ERROR: Data Difference")
            self.assertEqual(-6.415810105409037e-10, c2, "ERROR: "
                                                         "Data Difference")
            self.assertEqual('-6.41581e-10', c3, "ERROR: Data Difference")
            self.assertEqual('-6.41581e-10', c4, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_ForCast3(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT CAST (a_angle as byteint),CAST "
                            "(a_srno as smallint),"
                            "CAST (a_srno as bigint),CAST (a_srno as real),"
                            "CAST (a_angle as numeric (10,2)),CAST "
                            "(a_angle as char(5)),"
                            "CAST (a_angle as varchar(5)) FROM all_datatypes"
                            " ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5, c6, c7 in results:
            self.assertEqual(-1, c1, "ERROR: Data Difference")
            self.assertEqual(58, c2, "ERROR: Data Difference")
            self.assertEqual(58, c3, "ERROR: Data Difference")
            self.assertEqual(58, c4, "ERROR: Data Difference")
            self.assertEqual('-1.00', c5, "ERROR: Data Difference")
            self.assertEqual('-1   ', c6, "ERROR: Data Difference")
            self.assertEqual('-1', c7, "ERROR: Data Difference")

        self.cursor.execute("SELECT CAST (a_numps as int), "
                            "CAST (a_numps as smallint), "
                            "CAST (a_numps as bigint), CAST "
                            "(a_numps as real), "
                            "CAST (a_numps as numeric (4,0)), "
                            "CAST (a_numps as char(30)),"
                            " CAST (a_numps as varchar(30)) FROM "
                            "all_datatypes ORDER BY a_srno "
                            "limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5, c6, c7 in results:
            self.assertEqual(53, c1, "ERROR: Data Difference")
            self.assertEqual(53, c2, "ERROR: Data Difference")
            self.assertEqual(53, c3, "ERROR: Data Difference")
            self.assertEqual(53, c4, "ERROR: Data Difference")
            self.assertEqual('53', c5, "ERROR: Data Difference")
            self.assertEqual('53.00000000000000', c6, "ERROR: Data Difference")
            self.assertEqual('53.00000000000000', c7, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_ForLikeClause(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT a_srno, a_char5, a_date, a_timetz FROM "
                            "all_datatypes WHERE a_char5 LIKE '%%%%' AND"
                            " a_date LIKE  '1977______' AND a_timetz like"
                            " '%+%' ORDER BY a_srno limit 1 ")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual(6419, c1, "ERROR: Data Difference")
            self.assertEqual('xSgB ', c2, "ERROR: Data Difference")
            self.assertEqual('1977-05-10', c3, "ERROR: Data Difference")
            self.assertEqual('23:40:05+05:30', c4, "ERROR: Data Difference")

        self.cursor.execute("UPDATE all_datatypes SET a_char20 = "
                            "('%' || a_char20) WHERE a_srno = 8346")
        self.cursor.execute("SELECT a_srno,a_char20, a_char1000 FROM "
                            "all_datatypes "
                            "WHERE a_char20 LIKE '\\%%' escape '\\' "
                            "AND a_char1000 LIKE '%_%'")
        results = self.cursor.fetchall()
        for c1, c2, c3 in results:
            self.assertEqual(8346, c1, "ERROR: Data Difference")
            self.assertEqual('%trial', c2, "ERROR: Data Difference")
            self.assertEqual('0_ZX2lJLkyeM4Mn6PgEEh8QFfbQMw8obO jKAq55BaKHJgWb'
                             'c*S9YJ74ih2pdd/AbjPS/angwdovN5Q*2N-IZY '
                             'pQSiLTj:e8dHFq9Qk'
                             '8Nskc2*F31WV-.AcO9_A5dFmfteO79Qb:u+v:kEr'
                             'AdyonLMNzr/x6fN'
                             '1rKV6qPxoR. pUOQZD*:7zQSYh/WvAN ullT5RaWu'
                             '/8 a9g_XetY4+tz/'
                             'TYD75g.C+_Q+1r+SfVEsmh53Tew2Tj_qEry_srWs'
                             'k1PCNdqlEvGDrBFqw'
                             'zorZxRw/Kxr9VHuBQZJX4ss0XNsk.mMu_4KdOl'
                             '9CXfAUjGxnr6UC'
                             'kGxN uUY-492S+xTQ9Qn0ZSfuNBVPUJtPII'
                             'c4:Ca0VSR2:JbL4.2V*_qGfRI4I',
                             c3, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_SysFn(self):
        self.cursor.execute("SELECT current_catalog,current_user,"
                            "coalesce(NULL,10)")
        results = self.cursor.fetchall()
        for c1, c2, c3 in results:
            self.assertEqual('NZPY_TEST', c1, "ERROR: Data Difference")
            self.assertEqual('ADMIN', c2, "ERROR: Data Difference")
            self.assertEqual(10, c3, "ERROR: Data Difference")

    def test_StringFn1(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("select ascii(a_varchar1) ,"
                            "(octet_length(a_varchar1) * 8),"
                            "chr(a_srno) ,char_length(a_varchar50) ,"
                            "char_length(a_varchar50) ,"
                            "varcharcat(a_varchar1,a_varchar50) ,"
                            "lower(a_varchar1) ,"
                            "substring(a_varchar50,1,4) ,"
                            "length(a_varchar50) ,"
                            "position('m' in a_varchar50),"
                            "ltrim(a_varchar50) ,"
                            "octet_length(a_varchar50) ,"
                            "position('g' in a_varchar50) ,"
                            "repeat(a_varchar1,2),"
                            "ltrim(a_varchar50) ,lpad('',a_srno) ,"
                            "substring(a_varchar50,2,5) ,upper(a_varchar50) "
                            "from all_datatypes order by a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5, c6, c7, c8, c9, \
            c10, c11, c12, c13, c14, c15, c16, c17, c18 \
                in results:
            self.assertEqual(120, c1, "ERROR: Data Difference")
            self.assertEqual(8, c2, "ERROR: Data Difference")
            self.assertEqual(':', c3, "ERROR: Data Difference")
            self.assertEqual(26, c4, "ERROR: Data Difference")
            self.assertEqual(26, c5, "ERROR: Data Difference")
            self.assertEqual('xg.4+:_*/Y-066VKPLsgQxnQ/f5',
                             c6, "ERROR: Data Difference")
            self.assertEqual('x', c7, "ERROR: Data Difference")
            self.assertEqual('g.4+', c8, "ERROR: Data Difference")
            self.assertEqual(26, c9, "ERROR: Data Difference")
            self.assertEqual(0, c10, "ERROR: Data Difference")
            self.assertEqual('g.4+:_*/Y-066VKPLsgQxnQ/f5',
                             c11, "ERROR: Data Difference")
            self.assertEqual(26, c12, "ERROR: Data Difference")
            self.assertEqual(1, c13, "ERROR: Data Difference")
            self.assertEqual('xx', c14, "ERROR: Data Difference")
            self.assertEqual('g.4+:_*/Y-066VKPLsgQxnQ/f5',
                             c15, "ERROR: Data Difference")
            self.assertEqual('                                   '
                             '                       ', c16,
                             "ERROR: Data Difference")
            self.assertEqual('.4+:_', c17, "ERROR: Data Difference")
            self.assertEqual('G.4+:_*/Y-066VKPLSGQXNQ/F5',
                             c18, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_CountColumnWithMarker(self):
        self.performOperation("./setup_all_datatype")
        self.cursor.execute("SELECT COUNT(*) FROM all_datatypes "
                            "WHERE a_srno < ?", (1000,))
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual([4], c1, "ERROR: Data Difference")
        self.cursor.execute("SELECT COUNT(*) FROM all_datatypes"
                            " WHERE a_srno > ? ORDER BY COUNT(*) "
                            "LIMIT 1", (6419,))
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual([3], c1, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_TemporalDatatypes1(self):
        self.cursor.execute("SELECT date_part('day',2007-07-25)")
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual([1], c1, "ERROR: Data Difference")

        self.cursor.execute("SELECT date_part('dow',2007-07-25)")
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual([5], c1, "ERROR: Data Difference")

        self.cursor.execute("SELECT date_part('doy',2007-07-25),"
                            "date_part('MONTH',2007-07-25)")
        results = self.cursor.fetchall()
        for c1, c2 in results:
            self.assertEqual(1, c1, "ERROR: Data Difference")
            self.assertEqual(1, c2, "ERROR: Data Difference")

        self.cursor.execute("SELECT date_part('QUARTER',2007-07-25), "
                            "date_part('WEEK',2007-07-25),date_part"
                            "('YEAR',2007-07-25)")
        results = self.cursor.fetchall()
        for c1, c2, c3 in results:
            self.assertEqual(1, c1, "ERROR: Data Difference")
            self.assertEqual(1, c2, "ERROR: Data Difference")
            self.assertEqual(1970, c3, "ERROR: Data Difference")

    def test_TemporalDatatypes2(self):
        self.cursor.execute("CREATE TABLE tab (x integer,a date) ")
        self.cursor.execute("insert into tab  values (1, '2008-09-09')")
        self.cursor.execute("insert into tab  values (2, '1999-09-19')")
        self.cursor.execute("insert into tab  values (3, '2005-07-09')")
        self.cursor.execute("insert into tab  values (4, '1999-09-09')")
        self.cursor.execute("select x from tab where (date_part('YEAR',a) "
                            "= 1999) and (date_part('MONTH',a) = 9) and "
                            "(date_part('day',a) = 9)")
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual([4], c1, "ERROR: Data Difference")
        self.cursor.execute("drop table tab")

    def test_TemporalDatatypes3(self):
        self.cursor.execute("CREATE TABLE test1 (dateCol date) ")
        self.cursor.execute("Insert into test1 VALUES ('1977-10-05 BC')")
        self.cursor.execute("SELECT * FROM test1")
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual(['-1976-10-05'], c1, "ERROR: Data Difference")
        self.cursor.execute("DELETE FROM test1")
        self.cursor.execute("INSERT INTO test1 VALUES ('9999-12-30')")
        self.cursor.execute("SELECT  (dateCol + INTERVAL "
                            "'@ 1DAY') AS next_date FROM test1")
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual(['9999-12-31 00:00:00.0000'
                              '00'], c1, "ERROR: Data Difference")
        self.cursor.execute("DELETE FROM test1")
        self.cursor.execute("INSERT INTO test1 VALUES ('48010301 BC')")
        self.cursor.execute("SELECT * FROM test1")
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual(['-4800-03-01'], c1, "ERROR: Data Difference")
        self.cursor.execute("drop table test1")

    def test_LimitwithOffset(self):
        self.performOperation("./setup_all_datatype")
        expected = [[651], [698], [1628], [3336], [5631]]
        output = []
        self.cursor.execute("SELECT a_srno from all_datatypes "
                            "order by a_srno LIMIT 5 ,2")
        results = self.cursor.fetchall()
        for c1 in results:
            output.append(c1)
        self.assertEqual(output, expected, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_BoolCol(self):
        self.cursor.execute("CREATE TABLE all_datatypes (a_srno "
                            "integer,a_bool5 bool )")
        self.cursor.execute("Insert into all_datatypes values(10,'t')")
        self.cursor.execute("Insert into all_datatypes values(5,'0')")
        self.cursor.execute("Insert into all_datatypes values(12,'1')")
        self.cursor.execute("Insert into all_datatypes values(3,'f')")
        self.cursor.execute("SELECT * FROM all_datatypes "
                            "ORDER BY a_srno limit 1;")
        results = self.cursor.fetchall()
        for c1, c2 in results:
            self.assertEqual(3, c1, "ERROR: Data Difference")
            self.assertEqual(0, c2, "ERROR: Data Difference")
        self.cursor.execute("SELECT * FROM all_datatypes "
                            "ORDER BY a_srno DESC limit 1;")
        results = self.cursor.fetchall()
        for c1, c2 in results:
            self.assertEqual(12, c1, "ERROR: Data Difference")
            self.assertEqual(1, c2, "ERROR: Data Difference")
        self.cursor.execute("drop table all_datatypes")

    def test_Varbinary(self):
        self.cursor.execute("create table varbin_test (a_srno "
                            "int,col0 VARBINARY(50));")
        self.cursor.execute("insert into varbin_test values(?, "
                            "?)", (1, b'543b6c6c6f'))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("select col0 from varbin_test")
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual(['T;llo'], c1, "ERROR: Data Difference")
        self.cursor.execute("update varbin_test set col0 = ? where a_srno = "
                            "?", (b'543b6c6c6a', 1))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("select col0 from varbin_test")
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual(['T;llj'], c1, "ERROR: Data Difference")
        self.cursor.execute("delete from varbin_test where "
                            "col0 = ?", (b'543b6c6c6a',))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table varbin_test")

    def test_Geometry(self):
        self.cursor.execute("create table geometry_test (a_srno int,"
                            "col0 ST_GEOMETRY(50));")
        self.cursor.execute("insert into geometry_test values(?, "
                            "?)", (1, b'123d6c6f'))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("select col0 from geometry_test")
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual(['\x12=lo'], c1, "ERROR: Data Difference")
        self.cursor.execute("update geometry_test set col0 = ? where "
                            "a_srno = ?", (b'123d6c6a', 1))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("select col0 from geometry_test")
        results = self.cursor.fetchall()
        for c1 in results:
            self.assertEqual(['\x12=lj'], c1, "ERROR: Data Difference")
        self.cursor.execute("delete from geometry_test where col0 = ?",
                            (b'123d6c6a',))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table geometry_test")

    def test_ForFetchAllDataUtf8(self):
        self.performOperation("./setup_all_utf")
        self.cursor.execute("SELECT * FROM all_utf_datatypes ORDER BY "
                            "a_srno DESC limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual(15, c1, "ERROR: Data Difference")
            self.assertEqual('􃩝虲􄃨𨘊󻻶􎮟', c2, "ERROR: Data Difference")
            self.assertEqual('􆴩􁁭ꏫ󾆲𤍈󰘋󰘑', c4, "ERROR: Data Difference")
        self.cursor.execute("drop table all_utf_datatypes")

    def test_DeletetaUtf8(self):
        self.performOperation("./setup_all_utf")
        self.cursor.execute("delete from all_utf_datatypes where "
                            "a_nchar10 = ?", ('䄕莆',))
        self.assertEqual(1, self.cursor.rowcount, "ERROR: Data Difference")
        self.cursor.execute("drop table all_utf_datatypes")

    def test_Literals(self):
        self.performOperation("./setup_all_latin")
        self.cursor.execute("SELECT 'ODBC' || 'Testing' as test, "
                            "'Latin9' || a_char10, "
                            "'Netezza' a_char1000, 'Persistent' FROM "
                            "all_latin_datatypes "
                            "WHERE a_char1000 like  ? ORDER BY a_char10 "
                            "DESC limit 1", ('%Ã%',))
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual('ODBCTesting', c1, "ERROR: Data Difference")
            self.assertEqual('Latin9òÛì       ', c2, "ERROR: Data Difference")
            self.assertEqual('Netezza', c3, "ERROR: Data Difference")
            self.assertEqual('Persistent', c4, "ERROR: Data Difference")

        self.cursor.execute("SELECT 'Test Literals ',"
                            "(((((a_srno * 10 ) + 20 ) - 10 ) / 10)),"
                            "'complex operation' FROM all_latin_datatypes "
                            "ORDER BY a_char10 limit 1", ('%Ã%',))
        results = self.cursor.fetchall()
        for c1, c2, c3 in results:
            self.assertEqual('Test Literals ', c1, "ERROR: Data Difference")
            self.assertEqual(7, c2, "ERROR: Data Difference")
            self.assertEqual('complex operation', c3, "ERROR: Data Difference")

        self.cursor.execute("drop table all_latin_datatypes")

    def test_LatinAggregates(self):
        self.performOperation("./setup_all_latin")
        self.cursor.execute("SELECT count(*),count(a_char10) FROM "
                            "all_latin_datatypes;")
        results = self.cursor.fetchall()
        for c1, c2 in results:
            self.assertEqual(15, c1, "ERROR: Data Difference")
            self.assertEqual(14, c2, "ERROR: Data Difference")

        self.cursor.execute("SELECT count(*),count(a_char1000), "
                            "count(a_varchar10), count(a_varchar32767) "
                            "FROM all_latin_datatypes;")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual(15, c1, "ERROR: Data Difference")
            self.assertEqual(15, c2, "ERROR: Data Difference")
            self.assertEqual(15, c3, "ERROR: Data Difference")
            self.assertEqual(15, c4, "ERROR: Data Difference")

        self.cursor.execute("SELECT max (a_srno),max (a_char10), "
                            "min (a_srno), min (a_char10) "
                            "FROM all_latin_datatypes;")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4 in results:
            self.assertEqual(15, c1, "ERROR: Data Difference")
            self.assertEqual('òÛì       ', c2, "ERROR: Data Difference")
            self.assertEqual(1, c3, "ERROR: Data Difference")
            self.assertEqual('¢         ', c4, "ERROR: Data Difference")

        self.cursor.execute("drop table all_latin_datatypes")

    def test_UtfAggregates(self):
        self.performOperation("./setup_all_utf")
        self.cursor.execute("SELECT max (a_srno), max (a_nchar10), "
                            "max (a_nvarchar10), min (a_srno), "
                            "min (a_nchar10), "
                            "min (a_nvarchar10) FROM all_utf_datatypes;")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5, c6 in results:
            self.assertEqual(15, c1, "ERROR: Data Difference")
            self.assertEqual('􏜔󾄌', c2, "ERROR: Data Difference")
            self.assertEqual('􆴩􁁭ꏫ󾆲𤍈󰘋󰘑', c3, "ERROR: Data Difference")
            self.assertEqual(1, c4, "ERROR: Data Difference")
            self.assertEqual('', c5, "ERROR: Data Difference")
            self.assertEqual('埇䕼姲􁕢􅳶', c6, "ERROR: Data Difference")

        self.cursor.execute("SELECT max (a_srno),max (a_nchar1000) "
                            "FROM all_utf_datatypes;")
        results = self.cursor.fetchall()
        for c1, c2 in results:
            self.assertEqual(15, c1, "ERROR: Data Difference")
            self.assertEqual('􌘜乂畾𦼡􎵱􃥥􄬘􄠋󲼱𨗽𩌍􏍰󴓃󰆋𩨗뿡𦏠𩍨𧏏뗒􇷟󱱓􈩺􉁱󺻅픫璸󳃄𣢦󷙫󲢃󶼐󿌸􄎺蔤噮􀳉ꎸㅓ􇍦', c2,
                             "ERROR: Data Difference")

        self.cursor.execute("SELECT min (a_srno),min (a_nchar1000) "
                            "FROM all_utf_datatypes;")
        results = self.cursor.fetchall()
        for c1, c2 in results:
            self.assertEqual(1, c1, "ERROR: Data Difference")
            self.assertEqual('', c2, "ERROR: Data Difference")

        self.cursor.execute("drop table all_utf_datatypes")

    def test_ConcatLatin(self):
        self.performOperation("./setup_all_latin")
        self.cursor.execute("SELECT varcharcat(a_char10, a_varchar10), "
                            "varcharcat(a_varchar10, a_char10), "
                            "varcharcat(a_char10, a_char10) FROM "
                            "all_latin_datatypes "
                            "ORDER BY a_srno limit 1")
        results = self.cursor.fetchall()
        for c1, c2, c3 in results:
            self.assertEqual('¢         Ó£ì', c1, "ERROR: Data Difference")
            self.assertEqual('Ó£ì¢         ', c2, "ERROR: Data Difference")
            self.assertEqual('¢         ¢       '
                             '  ', c3, "ERROR: Data Difference")

        self.cursor.execute("drop table all_latin_datatypes")

    def test_FetchLatinUtf(self):
        self.performOperation("./setup_all_latin_utf")
        self.cursor.execute("select * from all_utfLatin_datatypes")
        results = self.cursor.fetchall()
        for c1, c2, c3, c4, c5, c6, c7 in results:
            self.assertEqual(1, c1, "ERROR: Data Difference")
            self.assertEqual('àë        ', c2, "ERROR: Data Difference")
            self.assertEqual('©¡ùâûôÞçå¶ý§ë¡êÆù×Òþ®²¬óßöÅè§ýÙ'
                             'åèáÓ¥ë³àûñ¥²ïú', c3, "ERROR: Data Difference")
            self.assertEqual('\xa0ì«¹Ü', c4,
                             "ERROR: Data Difference")
            self.assertEqual('î·®ð£', c5, "ERROR: Data Difference")
            self.assertEqual('ó´<9a><90>ð£¨°ð¥»<85>ô<8a>°¦ð¨<88>¾ó·¿<90>óµ'
                             '¿¡ô<85><8c>«ð<9d'
                             '><9b>»ô<8c>³<81>ê<8b>ªð <89>¨ô<8b><82><93>ô'
                             '<8c>½<9a>æ¢<8b>è°<9c>ô<', c6,
                             "ERROR: Data Difference")
            self.assertEqual('å', c7, "ERROR: Data Difference")
        self.cursor.execute("drop table all_utfLatin_datatypes")
