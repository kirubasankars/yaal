"""Shared data-provider lifecycle helpers (commit / rollback / close)."""


def commit_then_close(conn, *, release=None):
    """Commit then release the connection. On commit failure, rollback, release, re-raise.

    ``release(conn, *, close)`` receives close=False after a successful commit and
    close=True after a failed commit (so pooled connections can be discarded).
    If ``release`` is omitted, ``conn.close()`` is used.
    """
    if not conn:
        return
    try:
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        _release(conn, release=release, close=True)
        raise
    _release(conn, release=release, close=False)


def rollback_then_close(conn, *, release=None):
    """Rollback then release the connection (best-effort)."""
    if not conn:
        return
    try:
        conn.rollback()
    except Exception:
        pass
    _release(conn, release=release, close=True)


def _release(conn, *, release=None, close=True):
    if release is not None:
        try:
            release(conn, close=close)
        except Exception:
            pass
        return
    try:
        conn.close()
    except Exception:
        pass
