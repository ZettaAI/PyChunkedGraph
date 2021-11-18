import json
from messagingclient import MessagingClient
from cloudfiles import CloudFiles

from pychunkedgraph.graph import ChunkedGraph

# cf = CloudFiles("gs://akhilesh-pcg")
# ids = json.loads(cf.get("errors"))

graph_id = "minnie3_v1"
exchange = "test-queue"

c = MessagingClient()
cg = ChunkedGraph(graph_id=graph_id)
N = cg.id_client.get_max_operation_id()

attributes = {
    "graph_id": graph_id,
    "remesh_priority": "true",
}

print(N)
for i in range(N):
    data = str(i).encode("utf-8")
    c.publish(exchange, data, attributes)
