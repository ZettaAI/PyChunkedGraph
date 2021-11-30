from pychunkedgraph.utils.redis import get_redis_connection


def enqueue_operation_tasks(graph_id: str):
    from pychunkedgraph.utils.redis import get_rq_queue
    from pychunkedgraph.graph import ChunkedGraph
    from pychunkedgraph.ingest.fake_edges import fake_edge_task

    cg = ChunkedGraph(graph_id=graph_id)
    N = cg.id_client.get_max_operation_id()
    print(f"{N} jobs")
    for _id in range(N):
        fake_edges_q = get_rq_queue("fake")
        fake_edges_q.enqueue(
            fake_edge_task,
            job_id=f"{cg.graph_id}-{_id}",
            job_timeout="5m",
            result_ttl=0,
            args=(cg.graph_id, _id),
        )


if __name__ == "__main__":
    from sys import argv

    arg = argv[1]
    if arg == "status":
        redis = get_redis_connection()
        print(f"completed: {redis.hlen('c')}")
        print(f"fake_edge_chunks: {redis.get('fake_edge_chunks')}")
        print(f"errors: {redis.hlen('errors')}")
        print(f"merge_errors: {redis.hlen('merge_errors')}")
    else:
        enqueue_operation_tasks(argv[1])
