# nzpy: Pure python driver for IBM Netezza

## Scope

nzpy is a pure-Python IBM Netezza driver that complies with DB-API 2.0. It is tested on Python versions 3.5+. 

## Installation
To install nzpy using pip type:
``` pip install nzpy ```

To install nzpy using setup.py:
``` python setup.py install ```


## Interactive Example
This examples make use of the nzpy extensions to the DB-API 2.0 standard,

Import nzpy, connect to the database, create a table, add some rows and then query the table:
```
cursor = conn.cursor()
try:
    cursor.execute("create table t1(c1 numeric (10,5), c2 varchar(10),c3 nchar(5))")
    print("table created successfully")
except:
    print("Error while creating table")

cursor.execute("insert into t1 values (?,?,?)", (456.54,'abcd','abc'))
print(cursor.rowcount, 'products inserted')
cursor.execute("update t1 set c2 = 'uvw' where c2 = ?", ('abcd',))
print(cursor.rowcount, 'products updated')
cursor.execute("delete from t1 where c2 = ?", ('uvw',))
print(cursor.rowcount, 'products deleted')
cursor.close()
```

## Autocommit
As autocommit is on by default in IBM Netezza the default value of autocommit is on. It can be turned off by using the autocommit property of the connection.
```
conn.autocommit = False #autocommit is on by default. It can be turned off by using the autocommit property of the connection.
cursor = conn.cursor()
cursor.execute("create table t2(c1 numeric (10,5), c2 varchar(10),c3 nchar(5))")
cursor.execute("insert into t2 values (123.54,'xcfd','xyz')")
conn.rollback()
cursor.close()
```

## Notices
IBM Netezza notices are stored in a deque called Connection.notices and added using the append() method. Here’s an example:
```
cursor = conn.cursor()
cursor.execute("call CUSTOMER();")
print(conn.notices)

NOTICE: The customer name is alpha
```

## Logging 
You can set logLevel to control logging verbosity. In order to enable logging, you need to pass logLevel in your application using connection string. 
```
conn = nzpy.connect(user="admin", password="password",host='localhost', port=5480, database="db1", securityLevel=0, logLevel=0)
```
You can configure logLevel as per your requirement. If you skip initializing logLevel, it would take default value as 0.  
Valid values for 'logLevel' are : "0" for DEBUG , "1" for INFO and "2" for WARNING.
Logfile would be created in the same directory/folder in which your application would run.


## SecurityLevel 
The level of security (SSL/TLS) that the driver uses for the connection to the data store. 
```
onlyUnSecured: The driver does not use SSL. 
preferredUnSecured: If the server provides a choice, the driver does not use SSL. 
preferredSecured: If the server provides a choice, the driver uses SSL. 
onlySecured: The driver does not connect unless an SSL connection is available. 
```
Similarly, IBM Netezza server has above securityLevel. 

Cases which would fail: Client tries to connect with 'Only secured' or 'Preferred secured' mode while server is 'Only Unsecured' mode. Client tries to connect with 'Only secured' or 'Preferred secured' mode while server is 'Preferred Unsecured' mode. Client tries to connect with 'Only Unsecured' or 'Preferred Unsecured' mode while server is 'Only Secured' mode. Client tries to connect with 'Only Unsecured' or 'Preferred Unsecured' mode while server is 'Preferred Secured' mode. 

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
conn = nzpy.connect(user="admin", password="password",host='vmnps-dw10.svl.ibm.com', port=5480, database="db1", securityLevel=3, logLevel=0, ssl = {'ca_certs' : '/nz/cacert.pem'})
```
The above example opens a database handle on NPS server 'vmnps-dw10.svl.ibm.com'. nzpy driver should connect on port 5480(postgres port). The user is admin, password is password, database is db1 and the location of the ca certificate file is /nz/cacert.pem with securityLevel as 'Only Secured session' 

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

cursor = conn.cursor()
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
cursor = conn.cursor()
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

If you have any questions or issues you can create a new [issue here][issues].

Pull requests are very welcome! Make sure your patches are well tested.
Ideally create a topic branch for every separate change you make. For
example:

1. Fork the repo
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

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
