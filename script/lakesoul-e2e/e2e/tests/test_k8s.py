from e2etest.k8s import api_server_address


def test_k8s():
    addr = api_server_address()
    print(f"server: {addr}")
