from lakesoul.huggingface import from_lakesoul


def test_from_lakesoul():
    ds = from_lakesoul("part")
    cnt = 0
    for _ in ds:
        cnt += 1
    assert cnt == 20000
