import nzpy
import logging

logging.basicConfig(filename="myapplication.log")

conn = nzpy.connect(user="admin", password="password",host='ayush-nz1.fyre.ibm.com',
                    port=5480, database="system",
                    securityLevel=3,logLevel=0)

print(conn)
