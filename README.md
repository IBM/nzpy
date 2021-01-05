# nzpy: Pure python driver for IBM Netezza

## Scope

nzpy is a pure-Python IBM Netezza driver that complies with DB-API 2.0. It is tested on Python versions 3.5+. 

## Installation
To install nzpy using pip type:
```shell
pip install nzpy
```

To install nzpy using setup.py:
```shell
python setup.py install
```


## Interactive Example
This examples make use of the nzpy extensions to the DB-API 2.0 standard,

Import nzpy, connect to the database, create a table, add some rows and then query the table:

```python
import nzpy

conn = nzpy.connect(user="admin", password="password",host='localhost', port=5480, database="db1", securityLevel=1,logLevel=0)

with conn.cursor() as cursor:
    try:
        cursor.execute("create table customerAddress(Id int, Name varchar(10), Address varchar(50), Email varchar(20) )")
        print("Table customerAddress created successfully")
    except Exception as e:
        print(str(e))
    
    #insert data using parameters marker    
    cursor.execute("insert into customerAddress values (?,?,?,?)", (1,'Jack','High street, London', 'jack4321@ibm.com'))
    print(cursor.rowcount, 'rows inserted')
    cursor.execute("insert into customerAddress values (?,?,?,?)", (2,'Tom', 'Park street, NY','tom1234@ibm.com'))
    print(cursor.rowcount, 'rows inserted')
    cursor.execute("insert into customerAddress values (?,?,?,?)", (3,'James', 'MG street, SG','james678@ibm.com'))
    print(cursor.rowcount, 'rows inserted')

    # Using parameters (IMPORTANT: YOU SHOULD USE TUPLE TO PASS PARAMETERS)
    # Python note: a tuple with just one element must have a trailing comma, otherwise is just a enclosed variable
    cursor.execute("select * from customerAddress where Id = ? and Name = ?", (1,'Jack'))
    results = cursor.fetchall()
    for c1,c2,c3,c4 in results:
        print("Id = %s" % (c1))
        print("Name = %s" % (c2))
        print("Address = %s" % (c3))
        print("Email = %s" % (c4))

    try:
        cursor.execute("create table customerData(Id int, FirstName varchar(20), LastName varchar(20), Age int)")
        print("Table customerData created successfully")
    except Exception as e:
        print(str(e))
    
    #insert data using parameters marker in customerData table
    cursor.execute("insert into customerData values (?,?,?,?)", (1,'Jack','Bentley', 42))
    print(cursor.rowcount, 'rows inserted')
    cursor.execute("insert into customerData values (?,?,?,?)", (2,'Tom', 'Banks',28))
    print(cursor.rowcount, 'rows inserted')
    cursor.execute("insert into customerData values (?,?,?,?)", (3,'James', 'Grant',30))
    print(cursor.rowcount, 'rows inserted')

    cursor.execute("select ca.Id,cd.FirstName, cd.LastName, cd.Age, ca.Address, ca.Email from customerAddress ca, customerData cd where ca.Id = ? and ca.Id = cd.Id", (2,))
    results = cursor.fetchall()
    for c1,c2,c3,c4,c5,c6 in results:
        print("Id = %s" % (c1))
        print("FirstName = %s" % (c2))
        print("LastName = %s" % (c3))
        print("Age = %s" % (c4))
        print("Address = %s" % (c5))
        print("Email = %s" % (c6))
    
    # rowcount before
    cursor.execute("select count(*) from customerAddress")
    results = cursor.fetchall()
    for c1 in results:
        print("Table row count is %s" % (c1))
    
    # using remotesource 'python', create named external table and unload table data 
    try:
        cursor.execute("create external table et1 '/tmp/et10' using ( remotesource 'python' delimiter '|') as select * from customerAddress")
        print("Create external table created successfully")        
    except Exception as e:
        print(str(e))

    # load data from external table onto user table
    try:
        cursor.execute("insert into customerAddress select * from external '/tmp/et10' using ( remotesource 'python' delimiter '|' socketbufsize 8388608 ctrlchars 'yes'  encoding 'internal' timeroundnanos 'yes' crinstring 'off' maxerrors 3 LogDir '/tmp')")
        print("External Table loaded successfully")  
    except Exception as e:
        print(str(e))

    # rowcount after load from external table
    cursor.execute("select count(*) from customerAddress")
    results = cursor.fetchall()
    for c1 in results:
        print("After load from External Table, table row count is %s" % (c1))

```

## Autocommit
As autocommit is on by default in IBM Netezza the default value of autocommit is on. It can be turned off by using the autocommit property of the connection.

```python
conn.autocommit = False #autocommit is on by default. It can be turned off by using the autocommit property of the connection.
with conn.cursor() as cursor:
    cursor.execute("create table t2(c1 numeric (10,5), c2 varchar(10),c3 nchar(5))")
    cursor.execute("insert into t2 values (123.54,'xcfd','xyz')")
    conn.rollback()
```

## Notices
IBM Netezza notices are stored in a deque called cursor.notices and added using the append() method. Here’s an example:

```python
with conn.cursor() as cursor:
    cursor.execute("call CUSTOMER();")
    print(cursor.notices)

The customer name is alpha

```
If backend returns multiple notices:

```python
with conn.cursor() as cursor:
    cursor.execute("call CUSTOMER();")
    for notice in cursor.notices:
        print(notice)

The customer name is alpha
The customer location is beta

```

## Logging 
You can set logLevel to control logging verbosity. In order to enable logging, you need to pass logLevel in your application using connection string. 

```python
conn = nzpy.connect(user="admin", password="password",host='localhost', port=5480,
                       database="db1", securityLevel=0, logLevel=logging.DEBUG)
```

In addition there are 3 more options to control logging. One or more of these can be specified using `logOptions` argument to `nzpy.connect`

1. Inherit the logging settings of the parent / caller 

This is `nzpy.LogOptions.Inherit` option. The logging from nzpy is propgated to the logging configured by the parent

```python
logging.basicConfig(filename="myapplication.log")
logging.info("...")
# ..  
conn = nzpy.connect(user="admin", password="password",host='localhost', port=5480,
                    database="db1", securityLevel=0,
                    logOptions=nzpy.LogOptions.Inherit)

# .. all of nzpy logs will go to the inherited log settings
```

2. Logging details to a logfile

This is `nzpy.LogOptions.Logfile` option. The logging from nzpy is sent to 'nzpy.log' in the current directory. The file is rotated after 10 G. If `nzpy.LogOptions.Inherit` is set as well then both are honored

```python
logging.basicConfig(filename="myapplication.log")
logging.info("...")
# ..  
conn1 = nzpy.connect(user="admin", password="password",host='localhost', port=5480,
                     database="db1", securityLevel=0,
                     logOptions=nzpy.LogOptions.Logfile)

# .. all of conn1's nzpy logs will go to the nzpy.log only

conn2 = nzpy.connect(user="admin", password="password",host='localhost', port=5480,
                     database="db1", securityLevel=0,
                     logOptions=nzpy.LogOptions.Logfile | nzpy.LogOptions.Inherit)

# .. all of conn2's nzpy logs will go to the nzpy.log _and_ to myapplication.log


conn3 = nzpy.connect(user="admin", password="password",host='localhost', port=5480,
                     database="db1", securityLevel=0,
                     logOptions=nzpy.LogOptions.Disabled)
# .. conn3's logging is completely disabled

```

3. Disable nzpy logging

This is `nzpy.LogOptions.Disabled` option

You can configure logLevel as per your requirement. Any levels in standard `logging` module can be used. The default is `logging.INFO`


## SecurityLevel 
The level of security (SSL/TLS) that the driver uses for the connection to the data store. 
```
onlyUnSecured: The driver does not use SSL. 
preferredUnSecured: If the server provides a choice, the driver does not use SSL. 
preferredSecured: If the server provides a choice, the driver uses SSL. 
onlySecured: The driver does not connect unless an SSL connection is available. 
```
Similarly, IBM Netezza server has above securityLevel. 

Cases which would fail :
- Client tries to connect with 'Only secured' or 'Preferred secured' mode while server is 'Only Unsecured' mode
- Client tries to connect with 'Only secured' or 'Preferred secured' mode while server is 'Preferred Unsecured' mode
- Client tries to connect with 'Only Unsecured' or 'Preferred Unsecured' mode while server is 'Only Secured' mode
- Client tries to connect with 'Only Unsecured' or 'Preferred Unsecured' mode while server is 'Preferred Secured' mode 

Below is an example how you could pass securityLevel and ca certificate in connection string:
```
conn = nzpy.connect(user="admin", password="password",host='localhost', port=5480, database="db1", securityLevel=3, logLevel=0, ssl = {'ca_certs' : '/nz/cacert.pem'})
```
Below are the securityLevel you can pass in connection string : 
```
0: Preferred Unsecured session
1: Only Unsecured session
2: Preferred Secured session
3: Only Secured session
```

## Connection String 
Use connect to create a database connection with connection parameters: 
```
conn = nzpy.connect(user="admin", password="password",host='localhost', port=5480, database="db1", securityLevel=3, logLevel=0, ssl = {'ca_certs' : '/nz/cacert.pem'})
```
The above example opens a database handle on localhost. nzpy driver should connect on port 5480(postgres port). The user is admin, password is password, database is db1 and the location of the ca certificate file is /nz/cacert.pem with securityLevel as 'Only Secured session' 

**Connection Parameters**
When establishing a connection using nzgo you are expected to supply a connection string containing zero or more parameters. Below are subset of the connection parameters supported by nzgo. 
The following special connection parameters are supported: 
- database - The name of the database to connect to
- user - The user to sign in as
- password - The user's password
- host - The host to connect to. Values that start with / are for unix domain sockets. (default is localhost)
- port - The port to bind to. 
- securityLevel - Whether or not to use SSL (default is 0)
- ssl - Python dictionary containing location of the root certificate file. The file must contain PEM encoded data.


## Transactions 
As autocommit is on by default in IBM Netezza the default value of autocommit is on. It can be turned off by using the autocommit property of the connection.
```
conn.autocommit = False #This would internally called 'begin'

with conn.cursor() as cursor:
    cursor.execute("create table t2(c1 numeric (10,5), c2 varchar(10),c3 nchar(5))")
    conn.commit()  # This will commit create table transaction

    cursor.execute("insert into t2 values (123.54,'xcfd','xyz')")
    conn.rollback() # This will rollback insert into table transaction
```

## Supported Data Types 
This package returns the following types for values from the IBM Netezza backend: 
- integer types byteint, smallint, integer, and bigint are returned as int
- floating-point types real and double precision are returned as float
- character types char, varchar, nchar and nvarchar are returned as string
- temporal types date, time, timetz, timestamp, interval and timestamptz are
  returned as string
- numeric and geometry are returned as string
- the boolean type is returned as bool


## Parameter Style
nzpy do not support all the DB-API parameter styles. It only supports qmark style. Here’s an example of using the 'qmark' parameter style:
```
with conn.cursor() as cursor:
    cursor.execute("select * from t1 where c3 = ? and c2 = ?", ('abc','abcd'))
    results = cursor.fetchall()
    for c1, c2, c3 in results:
        print("c1 = %s, c2 = %s, c3 = %s" % (c1, c2, c3))
```

## External table 
You can unload data from an IBM Netezza database table on a Netezza host system to a remote client. This unload does not remove rows from the database but instead stores the unloaded data in a flat file (external table) that is suitable for loading back into a Netezza database. 
Below query would create a file 'et1.txt' on remote system from Netezza table t1 with data delimeted by '|'. 
```
cursor.execute("create external table et1 'C:\\et1.txt' using ( remotesource 'python' delimiter '|') as select * from t1")
```

## Features
- SSL/TLSv1.2 crypto support
- Transaction support: begin, rollback, commit
- Full support for all IBM Netezza data types
- Full DDL, DML query syntax support for IBM Netezza
- Full external table support (load and unload)
- Configurable logging feature
- Parameter style support


## Tests
To run tests, go to tests folder and run :

``` pytest -xv * ```

To run individual tests, you can run :

``` pytest -xv test_connection.py ```

## Contribution and help
All bug reports, feature requests and contributions are welcome at http://github.com/IBM/nzpy

If you have any questions or issues you can create a new [issue here][issues].

Pull requests are very welcome! Make sure your patches are well tested.
Ideally create a topic branch for every separate change you make. For
example:

1. Fork the repo (git clone https://github.com/IBM/nzpy.git)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
6. Assign any one of the reviewers:
   - abhishekjog
   - sandippawar1412
   - shabbir10july

<!-- License and Authors is optional here, but gives you the ability to highlight who is involed in the project -->
## License 

If you would like to see the detailed LICENSE click [here](LICENSE).

```text
Copyright:: 2019-2020 IBM, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

[issues]: https://github.com/IBM/nzpy/issues/new

