from lakesoul.ray.read_lakesoul import read_lakesoul


def test_ray_read_lakesoul():
    ds = read_lakesoul("part")
    cnt = 0
    for _ in ds.iter_rows():
        cnt += 1
    assert cnt == 20000
