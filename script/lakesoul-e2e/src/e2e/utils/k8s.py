# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
def api_server_address() -> str:
    from kubernetes import config

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
