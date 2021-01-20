from nzpy import core
from os import getpid, path
from hashlib import md5
from hashlib import sha256
import base64
from platform import system
from getpass import getuser
from socket import gethostname
from sys import argv
import logging

#CP Version
CP_VERSION_1 = 1
CP_VERSION_2 = 2
CP_VERSION_3 = 3
CP_VERSION_4 = 4
CP_VERSION_5 = 5
CP_VERSION_6 = 6

# Handshake version
HSV2_INVALID_OPCODE = 0
HSV2_CLIENT_BEGIN = 1
HSV2_DB = 2
HSV2_USER = 3 
HSV2_OPTIONS = 4
HSV2_TTY = 5
HSV2_REMOTE_PID = 6
HSV2_PRIOR_PID = 7
HSV2_CLIENT_TYPE = 8
HSV2_PROTOCOL = 9
HSV2_HOSTCASE = 10
HSV2_SSL_NEGOTIATE = 11 
HSV2_SSL_CONNECT = 12
HSV2_APPNAME = 13
HSV2_CLIENT_OS = 14
HSV2_CLIENT_HOST_NAME = 15 
HSV2_CLIENT_OS_USER = 16
HSV2_64BIT_VARLENA_ENABLED = 17
HSV2_CLIENT_DONE = 1000

# PG PROTOCOL
PG_PROTOCOL_3 = 3
PG_PROTOCOL_4 = 4
PG_PROTOCOL_5 = 5

# Authentication types 
AUTH_REQ_OK = 0
AUTH_REQ_KRB4 = 1
AUTH_REQ_KRB5 = 2
AUTH_REQ_PASSWORD = 3
AUTH_REQ_CRYPT = 4
AUTH_REQ_MD5 = 5
AUTH_REQ_SHA256 = 6

#Client type
NPS_CLIENT = 0
IPS_CLIENT = 1

#Client type python
NPSCLIENT_TYPE_PYTHON = 13

class Handshake():

    def __init__(self, _usock, _sock, ssl, log):
        
        self._hsVersion = None
        self._protocol1 = None
        self._protocol2 = None
        self._usock = _usock
        self._sock = _sock
        self.ssl_params = ssl
        self.log = log
        
        # guardium related information
        self.guardium_clientOS = system()
        self.guardium_clientOSUser = getuser()
        self.guardium_clientHostName = gethostname()
        self.guardium_applName = path.basename(argv[0])

    def startup(self, database, securityLevel, user, password):
   
        #Negotiate the handshake version (connection protocol)
        if not self.conn_handshake_negotiate(self._sock.write, self._sock.read, self._sock.flush, self._hsVersion, self._protocol2):
            self.log.info("Handshake negotiation unsuccessful")
            return False
            
        self.log.info("Sending handshake information to server")
        if not self.conn_send_handshake_info(self._sock.write, self._sock.read, self._sock.flush, database, securityLevel, self._hsVersion, self._protocol1, self._protocol2, user):  
            self.log.warning("Error in conn_send_handshake_info")
            return False
            
        if not self.conn_authenticate(self._sock.write, self._sock.read, self._sock.flush, password):
            self.log.warning("Error in conn_authenticate")
            return False
        
        if not self.conn_connection_complete(self._sock.read):
            self.log.warning("Error in conn_connection_complete")
            return False
            
        return self._sock    

    def conn_handshake_negotiate(self, _write, _read, _flush, _hsVersion, _protocol2):
    
        version = CP_VERSION_6
        self.log.debug ("Latest-handshake version (conn-protocol) = %s",version)
        
        while(1):
            
            if version == CP_VERSION_6:
                version = CP_VERSION_6
            
            if version == CP_VERSION_5:
                version = CP_VERSION_5
            
            if version == CP_VERSION_4:
                version = CP_VERSION_4
                
            if version == CP_VERSION_3:
                version = CP_VERSION_3
    
            if version == CP_VERSION_2:
                version = CP_VERSION_2
            
            self.log.debug("sending version: %s", version)
            val = bytearray( core.h_pack(HSV2_CLIENT_BEGIN) + core.h_pack(version))
            _write(core.i_pack(len(val) + 4))
            _write(val)
            _flush()
            
            self.log.info("sent handshake negotiation block successfully")
            
            beresp = _read(1)
            
            self.log.debug("Got response: %s", beresp)
            
            if beresp == b'N':
                self._hsVersion = version
                self._protocol2 = 0
                return True
            elif beresp == b'M':
                version = _read(1)
                if version == b'2':
                    version = CP_VERSION_2
                if version == b'3':
                    version = CP_VERSION_3
                if version == b'4':
                    version = CP_VERSION_4
                if version == b'5':
                    version = CP_VERSION_5
            elif beresp == b"E":
                self.log.warning("Bad attribute value error")
                return False
            else:
                self.log.warning("Bad protocol error")
                return False
    
    def conn_send_handshake_info(self, _write, _read, _flush, _database, securityLevel, _hsVersion, _protocol1, _protocol2, user):
        
        #We need database information at the backend in order to
        #select security restrictions. So always send the database first
        if not self.conn_send_database(_write, _read, _flush,_database):
            return False
            
        #If the backend supports security features and if the driver
        #requires secured session, negotiate security requirements now  
        if not self.conn_secure_session( securityLevel):
            return False
            
        if not self.conn_set_next_dataprotocol(self._protocol1, self._protocol2):
            return False
            
        if self._hsVersion == CP_VERSION_6 or self._hsVersion == CP_VERSION_4:
            return self.conn_send_handshake_version4(self._sock.write, self._sock.read, self._sock.flush, self._protocol1, self._protocol2, self._hsVersion, user)
        elif self._hsVersion == CP_VERSION_5 or self._hsVersion == CP_VERSION_3 or self._hsVersion == CP_VERSION_2:
            return self.conn_send_handshake_version2(self._sock.write, self._sock.read, self._sock.flush, self._protocol1, self._protocol2, self._hsVersion, user)
            
        return True
    
    def conn_send_database(self, _write,_read, _flush,_database):
    
        if _database is not None:
            if isinstance(_database, str):
                db = _database.encode('utf8')
            self.log.info("Database name: %s", db)
            
            val = bytearray( core.h_pack(HSV2_DB))
            val.extend(db + core.NULL_BYTE)
            _write(core.i_pack(len(val) + 4))
            _write(val)
            _flush()
        
        beresp = _read(1)
        self.log.debug("Backend response: %s", beresp)
        if beresp == b'N':
            return True
        elif beresp == core.ERROR_RESPONSE:
            self.log.warning("ERROR_AUTHOR_BAD")
            return False
        else:
            self.log.warning("Unknown response")
            return False
    
    def conn_set_next_dataprotocol(self, protocol1, _protocol2):
        
        if self._protocol2 == 0 :
            self._protocol1 = PG_PROTOCOL_3
            self._protocol2 = PG_PROTOCOL_5
    
        elif _protocol2 == 5 :
            self._protocol1 = PG_PROTOCOL_3
            self._protocol2 = PG_PROTOCOL_4
    
        elif _protocol2 == 4 :
            self._protocol1 = PG_PROTOCOL_3
            self._protocol2 = PG_PROTOCOL_3
            
        else: 
            return False
            
        self.log.debug("Connection protocol set to : %s %s",self._protocol1, self._protocol2)
        return True
    
    def conn_secure_session(self, securityLevel):
        
        information = HSV2_SSL_NEGOTIATE
        currSecLevel = securityLevel
        
        while information != 0 :
            
            if information == HSV2_SSL_NEGOTIATE:
                # SecurityLevel meaning
                # ---------------------------------------
                #      0	Preferred Unsecured session
                #      1	Only Unsecured session
                #      2	Preferred Secured session
                #      3	Only Secured session
                #
                self.log.debug("Security Level requested = %s",currSecLevel)
                opcode = information
                
            if information == HSV2_SSL_CONNECT:
                opcode = information                            
                 
            val = bytearray( core.h_pack(opcode) + core.i_pack(currSecLevel))
            self._sock.write(core.i_pack(len(val) + 4))
            self._sock.write(val)
            self._sock.flush()                     

            if information == HSV2_SSL_CONNECT:
                try:
                    self._usock = ssl_context.wrap_socket(self._usock)                        
                    self._sock = self._usock.makefile(mode="rwb")
                    self.log.info("Secured Connect Success")
                except ssl.SSLError:
                    self.log.warning("Problem establishing secured session")
                    return False    
                
            if information != 0:
                beresp = self._sock.read(1)
                self.log.debug("Got response =%s", beresp)
                if beresp == b'S':
                    #The backend sends 'S' only in 3 cases
                    #Client requests strict SSL and backend supports it.
                    #Client requests preffered SSL and backend supports it.
                    #Client requests preffered non-SSL, but backend supports
                    #only secured sessions.

                    if not isinstance(self.ssl_params, dict):
                        self.ssl_params = {}
                    try:
                        import ssl
                        
                        ca_certs = self.ssl_params.get('ca_certs')
                        ssl_context = ssl.create_default_context(cafile=ca_certs)                                                           
                        ssl_context.check_hostname = False
                        if ca_certs is None:
                            ssl_context.verify_mode = ssl.CERT_NONE
                        else:
                            ssl_context.verify_mode = ssl.CERT_REQUIRED            

                        information = HSV2_SSL_CONNECT
                        
                    except ImportError:
                        raise InterfaceError( "SSL required but ssl module not available in "
                                "this python installation")
                    
                    except ssl.SSLError:
                        if currSecLevel == 2:
                            self.log.debug("Problem establishing secured session")
                            self.log.debug("Attempting unsecured session")
                            currSecLevel = 1
                            information = HSV2_SSL_NEGOTIATE
                            continue
                        self.log.warning("Problem establishing secured session")
                        return False
    
                if beresp == b'N':
                    if information == HSV2_SSL_NEGOTIATE:
                        self.log.info("Attempting unsecured session")
                    information = 0
                    return True
                    
                if beresp == b'E':
                    #If 'E' is received, because the SSL session establishment
                    #failed, and we are requesting preffered secured session,
                    #we will have to attempt a non secured session. If this also
                    #fails, we have to error out. To achieve this, we now negotiate
                    #for essential non-secured session
                    self.log.warning("Error: connection failed")
                    return False         
    
    def conn_send_handshake_version2(self, _write, _read, _flush, _protocol1, _protocol2, _hsVersion, user):
        
        if isinstance(user, str):
            user = user.encode('utf8')
        else:
            user = user
            
        information = HSV2_USER             
        val = bytearray( core.h_pack(information))
        val.extend(user + core.NULL_BYTE)
        information = HSV2_PROTOCOL
        
        while information != 0 :    
            
            _write(core.i_pack(len(val) + 4))
            _write(val)
            _flush()
            beresp = _read(1)
            self.log.debug("Backend response: %s",beresp)
            if beresp == b'N':
                if information == HSV2_PROTOCOL:
                    val = bytearray( core.h_pack(information) + core.h_pack(_protocol1) + core.h_pack(_protocol2))
                    information = HSV2_REMOTE_PID                    
                    continue
        
                if information == HSV2_REMOTE_PID:
                    val = bytearray( core.h_pack(information) + core.i_pack(getpid()))
                    information = HSV2_CLIENT_TYPE
                    continue
                    
                if information == HSV2_CLIENT_TYPE:    
                    val = bytearray( core.h_pack(information) + core.h_pack(NPSCLIENT_TYPE_PYTHON))         
                    if _hsVersion == CP_VERSION_5 or _hsVersion == CP_VERSION_6:
                        information = HSV2_64BIT_VARLENA_ENABLED
                    else:
                        information = HSV2_CLIENT_DONE
                    continue
                
                if information == HSV2_64BIT_VARLENA_ENABLED:
                    val = bytearray( core.h_pack(information) + core.h_pack(IPS_CLIENT))  
                    information = HSV2_CLIENT_DONE
                    continue
                
                if information == HSV2_CLIENT_DONE:
                    val = bytearray( core.h_pack(information))
                    information = 0
                    _write(core.i_pack(len(val) + 4))
                    _write(val)
                    _flush()
                    return True
                
            elif beresp == core.ERROR_RESPONSE:
                self.log.warning("ERROR_CONN_FAIL")
                return False                
    
    def conn_send_handshake_version4(self, _write, _read, _flush, _protocol1, _protocol2, _hsVersion, user):
        
        if isinstance(user, str):
            user = user.encode('utf8')
        else:
            user = user
            
        information = HSV2_USER             
        val = bytearray( core.h_pack(information))
        val.extend(user + core.NULL_BYTE)
        information = HSV2_APPNAME
        
        while information != 0 :    
            
            _write(core.i_pack(len(val) + 4))
            _write(val)
            _flush()
            beresp = _read(1)
            self.log.debug("Backend response: %s",beresp)
            if beresp == b'N':             
                if information == HSV2_APPNAME: # App name 
                    val = bytearray( core.h_pack(information))
                    val.extend(self.guardium_applName.encode('utf8') + core.NULL_BYTE)                                 
                    self.log.debug("Appname :%s", self.guardium_applName)
                    information = HSV2_CLIENT_OS
                    continue

                if information == HSV2_CLIENT_OS: # OS name
                    val = bytearray( core.h_pack(information))
                    val.extend(self.guardium_clientOS.encode('utf8') + core.NULL_BYTE)                                 
                    self.log.debug("Client OS :%s", self.guardium_clientOS)
                    information = HSV2_CLIENT_HOST_NAME
                    continue

                if information == HSV2_CLIENT_HOST_NAME: # Client Host name
                    val = bytearray( core.h_pack(information))
                    val.extend(self.guardium_clientHostName.encode('utf8') + core.NULL_BYTE)                                 
                    self.log.debug("Client hostname :%s", self.guardium_clientHostName)
                    information = HSV2_CLIENT_OS_USER
                    continue

                if information == HSV2_CLIENT_OS_USER: # client OS User name 
                    val = bytearray( core.h_pack(information))
                    val.extend(self.guardium_clientOSUser.encode('utf8') + core.NULL_BYTE)                                 
                    self.log.debug("Client OS user :%s", self.guardium_clientOSUser)
                    information = HSV2_PROTOCOL
                    continue

                if information == HSV2_PROTOCOL:
                    val = bytearray( core.h_pack(information) + core.h_pack(_protocol1) + core.h_pack(_protocol2))
                    information = HSV2_REMOTE_PID                    
                    continue
                    
                if information == HSV2_REMOTE_PID:
                    val = bytearray( core.h_pack(information) + core.i_pack(getpid()))
                    information = HSV2_CLIENT_TYPE
                    continue
                    
                if information == HSV2_CLIENT_TYPE:    
                    val = bytearray( core.h_pack(information) + core.h_pack(NPSCLIENT_TYPE_PYTHON))         
                    if _hsVersion == CP_VERSION_5 or _hsVersion == CP_VERSION_6:
                        information = HSV2_64BIT_VARLENA_ENABLED
                    else:
                        information = HSV2_CLIENT_DONE
                    continue
                
                if information == HSV2_64BIT_VARLENA_ENABLED:
                    val = bytearray( core.h_pack(information) + core.h_pack(IPS_CLIENT))  
                    information = HSV2_CLIENT_DONE
                    continue
                
                if information == HSV2_CLIENT_DONE:
                    val = bytearray( core.h_pack(information))
                    information = 0
                    _write(core.i_pack(len(val) + 4))
                    _write(val)
                    _flush()
                    return True
                
            elif beresp == core.ERROR_RESPONSE:
                self.log.warning("ERROR_CONN_FAIL")
                return False                

    def conn_authenticate(self, _write, _read, _flush, password):
        
        if isinstance(password, str):
            password = password.encode('utf8')
        else:
            password = password
            
        beresp = _read(1)
        self.log.debug("Got response: %s", beresp)
        
        if beresp != core.AUTHENTICATION_REQUEST :
            self.log.warning("Authentication error")
            return False
        
        self.log.debug("auth got 'R' - request for password")
        areq = core.i_unpack(_read(4))[0]
        self.log.debug("areq =%s",areq)
        
        if areq == AUTH_REQ_OK:
            self.log.info("success")
            return True
        
        if areq == AUTH_REQ_PASSWORD:
            self.log.info("Plain password requested")
            _write(core.i_pack(len(password + core.NULL_BYTE) + 4))
            _write(password + core.NULL_BYTE)
            _flush()
        
        if areq == AUTH_REQ_MD5:
            self.log.info("Password type is MD5")
            salt = _read(2)
            self.log.debug("Salt =%s", salt)
            if password is None:
                raise InterfaceError(
                    "server requesting MD5 password authentication, but no "
                    "password was provided")
            md5encoded = md5(salt+password)
            md5pwd = base64.standard_b64encode(md5encoded.digest())
            pwd = md5pwd.rstrip(b"=")
            self.log.debug("md5 encrypted password is =%s",pwd)
            
            # Int32 - Message length including self.
            # String - The password.  Password may be encrypted.
            _write(core.i_pack(len(pwd + core.NULL_BYTE) + 4))
            _write(pwd + core.NULL_BYTE)
            _flush()
        
        if areq == AUTH_REQ_SHA256:
            self.log.info("Password type is SSH")
            salt = _read(2)
            self.log.debug("Salt =%s", salt)
            if password is None:
                raise InterfaceError(
                    "server requesting MD5 password authentication, but no "
                    "password was provided")
            sha256encoded = sha256(salt+password)
            sha256pwd = base64.standard_b64encode(sha256encoded.digest())
            pwd = sha256pwd.rstrip(b"=")
            self.log.debug("sha256 encrypted password is =%s", pwd)
            
            # Int32 - Message length including 
            # String - The password.  Password may be encrypted.
            _write(core.i_pack(len(pwd + core.NULL_BYTE) + 4))
            _write(pwd + core.NULL_BYTE)
            _flush()
            
        if areq == AUTH_REQ_KRB5:
            self.log.info("krb encryption requested from backend")
    
        return True
    
    def conn_connection_complete(self, _read):            
        while (1):
            response = _read(1)
            self.log.debug("backend response: %s", response)
            
            if response != core.AUTHENTICATION_REQUEST:
                _read(4) # do not use just ignore
                length = core.i_unpack(_read(4))[0]
            
            if response == core.AUTHENTICATION_REQUEST:
                areq = core.i_unpack(_read(4))[0]
                self.log.debug("backend response: %s", areq)

            if response == core.NOTICE_RESPONSE:
                notices = str(_read(length),'utf8')
                self.log.debug ("Response received from backend:%s", notices)

            if response == core.BACKEND_KEY_DATA:
                
                areq = core.i_unpack(_read(4))[0]
                self.log.debug("Backend response PID: %s", areq)
    
                areq = core.i_unpack(_read(4))[0]
                self.log.debug("Backend response KEY: %s", areq)
    
            if response == core.READY_FOR_QUERY:
                self.log.info("Authentication Successful")
                return True
            
            if response == core.ERROR_RESPONSE:
                error = str(_read(length),'utf8')
                self.log.warning("Error occured, server response:%s", error)
                return False
   
    
