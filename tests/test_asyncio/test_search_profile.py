"""Tests for awaiting direct FT.PROFILE replies and preserving pipeline queuing."""

from unittest.mock import AsyncMock, Mock, sentinel

import pytest

from redis.asyncio import Redis
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search.query import Query


@pytest.mark.fixed_client
@pytest.mark.parametrize("aggregate", [False, True])
@pytest.mark.parametrize("limited", [False, True])
async def test_profile_awaits_reply(aggregate, limited):
    client = Redis(protocol=2)
    search = client.ft("profile-index")
    search.execute_command = AsyncMock(return_value=sentinel.reply)
    search._parse_results = Mock(return_value=sentinel.result)
    query = AggregateRequest("*") if aggregate else Query("$term")

    result = await search.profile(
        query, limited=limited, query_params={"term": "hello"}
    )

    assert result is sentinel.result
    search.execute_command.assert_awaited_once()
    command = search.execute_command.await_args.args
    assert command[:3] == (
        "FT.PROFILE",
        "profile-index",
        "AGGREGATE" if aggregate else "SEARCH",
    )
    assert ("LIMITED" in command) is limited
    if not aggregate:
        assert command[-4:] == ("PARAMS", 2, "term", "hello")
    search._parse_results.assert_called_once()
    args, options = search._parse_results.call_args
    assert args == ("FT.PROFILE", sentinel.reply)
    assert options["query"] is query
    assert options["duration"] >= 0


@pytest.mark.fixed_client
async def test_profile_propagates_command_error():
    search = Redis().ft()
    search.execute_command = AsyncMock(side_effect=RuntimeError("profile failed"))
    search._parse_results = Mock()

    with pytest.raises(RuntimeError, match="profile failed"):
        await search.profile(Query("*"))

    search.execute_command.assert_awaited_once()
    search._parse_results.assert_not_called()


@pytest.mark.fixed_client
@pytest.mark.parametrize("module_pipeline", [False, True])
@pytest.mark.parametrize("aggregate", [False, True])
@pytest.mark.parametrize("limited", [False, True])
async def test_profile_keeps_synchronous_pipeline_queuing(
    module_pipeline, aggregate, limited
):
    client = Redis()
    pipeline = (
        client.ft("profile-index").pipeline() if module_pipeline else client.pipeline()
    )
    search = pipeline if module_pipeline else pipeline.ft("profile-index")

    query = AggregateRequest("*") if aggregate else Query("*")
    assert search.profile(query, limited=limited) is pipeline
    assert len(pipeline.command_stack) == 1
    assert pipeline.command_stack[0][0][:3] == (
        "FT.PROFILE",
        "profile-index",
        "AGGREGATE" if aggregate else "SEARCH",
    )
    assert ("LIMITED" in pipeline.command_stack[0][0]) is limited
    await pipeline.reset()


@pytest.mark.fixed_client
@pytest.mark.parametrize("aggregate", [False, True])
async def test_profile_parses_awaited_resp2_reply(aggregate):
    search = Redis(protocol=2).ft()
    query = (
        AggregateRequest("*")
        if aggregate
        else Query("*").return_field("blob", decode_field=False)
    )
    rows = (
        [1, [b"text", b"hello"]] if aggregate else [1, b"doc:1", [b"blob", b"\xff\x00"]]
    )
    search.execute_command = AsyncMock(return_value=[rows, []])

    result, _ = await search.profile(query)

    if aggregate:
        assert result.rows == [[b"text", b"hello"]]
    else:
        assert result.total == 1
        assert result.docs[0].blob == b"\xff\x00"
    search.execute_command.assert_awaited_once()


@pytest.mark.fixed_client
async def test_profile_rejects_invalid_query_before_execution():
    search = Redis().ft()
    search.execute_command = AsyncMock()

    with pytest.raises(ValueError, match="Must provide AggregateRequest"):
        await search.profile("*")

    search.execute_command.assert_not_called()
