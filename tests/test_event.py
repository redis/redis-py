from unittest.mock import Mock, AsyncMock

from redis.event import (
    EventListenerInterface,
    EventDispatcher,
    AsyncEventListenerInterface,
)


class TestEventDispatcher:
    def test_register_listeners(self):
        mock_event = Mock(spec=object)
        mock_event_listener = Mock(spec=EventListenerInterface)
        listener_called = 0

        def callback(event):
            nonlocal listener_called
            listener_called += 1

        mock_event_listener.listen = callback

        # Register via constructor
        dispatcher = EventDispatcher(
            event_listeners={type(mock_event): [mock_event_listener]}
        )
        dispatcher.dispatch(mock_event)

        assert listener_called == 1

        # Register additional listener for the same event
        mock_another_event_listener = Mock(spec=EventListenerInterface)
        mock_another_event_listener.listen = callback
        dispatcher.register_listeners(
            event_listeners={type(mock_event): [mock_another_event_listener]}
        )
        dispatcher.dispatch(mock_event)

        assert listener_called == 3

    async def test_register_listeners_async(self):
        mock_event = Mock(spec=object)
        mock_event_listener = AsyncMock(spec=AsyncEventListenerInterface)
        listener_called = 0

        async def callback(event):
            nonlocal listener_called
            listener_called += 1

        mock_event_listener.listen = callback

        # Register via constructor
        dispatcher = EventDispatcher(
            event_listeners={type(mock_event): [mock_event_listener]}
        )
        await dispatcher.dispatch_async(mock_event)

        assert listener_called == 1

        # Register additional listener for the same event
        mock_another_event_listener = Mock(spec=AsyncEventListenerInterface)
        mock_another_event_listener.listen = callback
        dispatcher.register_listeners(
            event_listeners={type(mock_event): [mock_another_event_listener]}
        )
        await dispatcher.dispatch_async(mock_event)

        assert listener_called == 3
