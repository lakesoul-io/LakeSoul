import time
import threading
import re
from typing import Set
from kubernetes import client, config, watch


def get_jobs(req, ids):
    """get job_id from k8s flink pod's log
    Args:
        req (_type_): _description_
        ids (_type_): _description_
    """
    logs = req.get()
    jobids = re.findall(r"JobId=([0-9a-f]{32})", logs)
    for jid in jobids:
        ids.add(jid)


def flink_wait_until_finish(label_selector, timeout) -> Set[str]:
    """use k8s to wait flink job finish

    Args:
        label_selector (_type_): _description_
        timeout (_type_): _description_

    Raises:
        RuntimeError: _description_
        RuntimeError: _description_

    Returns:
        Set[str]: flink job ids
    """
    config.load_kube_config()
    v1 = client.CoreV1Api()
    w = watch.Watch()
    finished = False
    thread = None
    ids = set()
    for e in w.stream(
        v1.list_namespaced_pod,
        namespace="default",
        label_selector=label_selector,
        timeout_seconds=timeout,
    ):
        typ = e.get("type")  # type: ignore
        obj = e.get("object")  # type: ignore
        if typ == "ADDED":
            name = obj.metadata.name  # # type: ignore
            namespace = obj.metadata.namespace  # # type: ignore
            start_time = time.time()
            while time.time() - start_time < timeout:
                pod = v1.read_namespaced_pod(name=name, namespace=namespace)
                container_statuses = pod.status.container_statuses or []  # type: ignore
                if container_statuses[0].state.running is not None:
                    break
                time.sleep(1)
            req = v1.read_namespaced_pod_log(
                async_req=True, name=name, namespace=namespace, follow=True
            )
            thread = threading.Thread(target=get_jobs, args=(req, ids))
            thread.start()
        elif e.get("type") == "DELETED":  # type: ignore
            finished = True
            break

    if not finished:
        raise RuntimeError("Spy on k8s failed")

    if thread is None:
        raise RuntimeError("Can not read pod logs")
    thread.join()
    return ids


def api_server_address() -> str:
    config.load_kube_config()
    _, active_context = config.list_kube_config_contexts()
    if not active_context:
        raise RuntimeError("没有找到有效的 kubeconfig 上下文")
    cluster_info = active_context.get("context", {}).get("cluster")
    kube_config = config.kube_config.KubeConfigMerger(
        config.KUBE_CONFIG_DEFAULT_LOCATION
    )
    cluster = kube_config.config["clusters"]  # type: ignore
    api_server = None
    for c in cluster:
        if c["name"] == cluster_info:
            api_server = c["cluster"]["server"]
            break
    if api_server is None:
        raise RuntimeError("can not load k8s")
    return api_server  # type: ignore
