from lakesoul._lib._context import create_lakesoul_session_ctx_internal


def create_lakesoul_session_ctx():
    from datafusion import SessionContext
    ctx = SessionContext()
    ctx.ctx = create_lakesoul_session_ctx_internal()
    return ctx
