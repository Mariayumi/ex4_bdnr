from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config= {
         'secure_connect_bundle': 'secure-connect-cassandra.zip'
}
auth_provider = PlainTextAuthProvider('ZJZOpmlgllBFRPgIsqRuRqlb', 'vyQMUx8ggESlZ890qk8+.I-YMRwww9bgG-34iO9qwnSx1,NHdic,Sgel8vPufQPXCGN7,_.WmU43R.5DL+y8+FLx7XA.3gydR15kkolTC+RH_bU74j,ncFN6HHR24a.a')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
      print(row[0])
else:
      print("An error occurred.")