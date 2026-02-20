
import libsql_client
print(dir(libsql_client))
try:
    print(help(libsql_client.create_client))
except:
    pass
