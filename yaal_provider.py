"""Shared data-provider lifecycle helpers (commit / rollback / close) and row streaming."""


DEFAULT_FETCH_BATCH_SIZE = 1000


def fetch_dict_rows(cursor, *, batch_size=DEFAULT_FETCH_BATCH_SIZE):
    """Drain a DB-API cursor into a list of row dicts via fetchmany (no fetchall).

    Assumes the cursor already yields mapping rows (e.g. sqlite row_factory,
    RealDictCursor, MySQL dictionary=True). Returns [] when there is no result set.
    """
    if cursor.description is None:
        return []
    rows = []
    size = batch_size if batch_size and batch_size > 0 else DEFAULT_FETCH_BATCH_SIZE
    while True:
        batch = cursor.fetchmany(size)
        if not batch:
            break
        rows.extend(batch)
    return rows


def fetch_mapped_rows(rows_raw, column_names, *, batch_size=DEFAULT_FETCH_BATCH_SIZE):
    """Build row dicts from a sequence of tuples, in batches (for drivers without cursors)."""
    rows = []
    size = batch_size if batch_size and batch_size > 0 else DEFAULT_FETCH_BATCH_SIZE
    buf = []
    for raw in rows_raw:
        buf.append(dict(zip(column_names, raw)))
        if len(buf) >= size:
            rows.extend(buf)
            buf = []
    if buf:
        rows.extend(buf)
    return rows


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


def parse_pool_int(query, key, default, *, minimum=1, maximum=32):
    """Parse a positive int pool setting from a URL query dict."""
    query = query or {}
    if key not in query:
        return default
    try:
        value = int(query[key])
    except (TypeError, ValueError):
        return default
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value
