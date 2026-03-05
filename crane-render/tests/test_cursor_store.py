"""Tests for cursor persistence."""

from unittest.mock import MagicMock

from crane_render.reader.cursor_store import CursorStore, DEFAULT_CURSOR


def _make_store():
    mock_redis = MagicMock()
    mock_client = MagicMock()
    mock_client.client = mock_redis
    return CursorStore(mock_client, key="test:cursors"), mock_redis


def test_get_returns_default_for_unknown_topic():
    store, mock = _make_store()
    mock.hget.return_value = None

    assert store.get("crane:events:raw_quotes") == DEFAULT_CURSOR
    mock.hget.assert_called_once_with("test:cursors", "crane:events:raw_quotes")


def test_get_returns_stored_cursor():
    store, mock = _make_store()
    mock.hget.return_value = b"1709654400000-5"

    assert store.get("crane:events:raw_quotes") == "1709654400000-5"


def test_get_returns_string_cursor():
    store, mock = _make_store()
    mock.hget.return_value = "1709654400000-5"

    assert store.get("crane:events:raw_quotes") == "1709654400000-5"


def test_save_writes_to_hash():
    store, mock = _make_store()
    store.save("crane:events:raw_quotes", "1709654400000-10")

    mock.hset.assert_called_once_with("test:cursors", "crane:events:raw_quotes", "1709654400000-10")


def test_save_batch_writes_mapping():
    store, mock = _make_store()
    cursors = {
        "crane:events:raw_quotes": "100-0",
        "crane:events:raw_options": "200-0",
    }
    store.save_batch(cursors)

    mock.hset.assert_called_once_with("test:cursors", mapping=cursors)


def test_save_batch_skips_empty():
    store, mock = _make_store()
    store.save_batch({})

    mock.hset.assert_not_called()


def test_get_all():
    store, mock = _make_store()
    mock.hget.side_effect = [b"100-0", None, b"300-0"]

    topics = ["t1", "t2", "t3"]
    result = store.get_all(topics)

    assert result == {"t1": "100-0", "t2": DEFAULT_CURSOR, "t3": "300-0"}
