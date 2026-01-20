from lakesoul.datafusion import create_lakesoul_session_ctx


def test_write():
    ctx = create_lakesoul_session_ctx()
    ctx.sql("show tables").show()
    # ctx.sql("create external table test_table (id int, name string) stored as lakesoul location '/tmp/test_table'").show()
    # ctx.sql("insert into test_table values (1, 'John'), (2, 'Jane')").show()
    ctx.sql("select * from test_table").show()
