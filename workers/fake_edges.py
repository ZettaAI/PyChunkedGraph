from messagingclient import MessagingClient


def callback(payload):
    from datetime import timedelta
    from pychunkedgraph.graph import ChunkedGraph
    from pychunkedgraph.graph.operation import GraphEditOperation
    from pychunkedgraph.graph.operation import MergeOperation
    from pychunkedgraph.graph.attributes import OperationLogs
    from pychunkedgraph.utils.redis import get_redis_connection
    from pychunkedgraph.graph.exceptions import PreconditionError

    redis = get_redis_connection()

    operation_id = int(payload.data.decode())
    graph_id = payload.attributes["graph_id"]
    cg = ChunkedGraph(graph_id=graph_id)

    try:
        log, ts = cg.client.read_log_entry(operation_id)
        if not log:
            return
    except Exception:
        redis.hset("errors", f"{operation_id}", "")
        return

    try:
        op = GraphEditOperation.from_log_record(cg, log)
    except Exception:
        redis.hset("errors", f"{operation_id}", "")
        return

    ts = ts - timedelta(milliseconds=500)
    op.parent_ts = ts

    try:
        status = log[OperationLogs.Status]
    except KeyError:
        status = 0

    if not isinstance(op, MergeOperation) or status:
        return

    try:
        _, rows = op._apply(operation_id=operation_id, timestamp=ts)
    except PreconditionError:
        redis.hset("merge_errors", f"{operation_id}", "")
        return

    if len(rows):
        cg.client.write(rows)
        redis.incr("fake_edge_chunks", amount=1)


c = MessagingClient()
c.consume("fake-edges", callback)
