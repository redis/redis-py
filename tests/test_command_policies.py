from unittest.mock import Mock

import pytest

from redis._parsers import CommandsParser
from redis._parsers.commands import CommandPolicies, RequestPolicy, ResponsePolicy
from redis.commands.policies import DynamicPolicyResolver, StaticPolicyResolver


@pytest.mark.onlycluster
class TestBasePolicyResolver:
    def test_resolve(self):
        mock_command_parser = Mock(spec=CommandsParser)
        zcount_policy = CommandPolicies(request_policy=RequestPolicy.DEFAULT_KEYED, response_policy=ResponsePolicy.DEFAULT_KEYED)
        rpoplpush_policy = CommandPolicies(request_policy=RequestPolicy.DEFAULT_KEYED, response_policy=ResponsePolicy.DEFAULT_KEYED)

        mock_command_parser.get_command_policies.return_value = {
            'core': {
                'zcount': zcount_policy,
                'rpoplpush': rpoplpush_policy,
            }
        }

        dynamic_resolver = DynamicPolicyResolver(mock_command_parser)
        assert dynamic_resolver.resolve('zcount') == zcount_policy
        assert dynamic_resolver.resolve('rpoplpush') == rpoplpush_policy

        with pytest.raises(ValueError, match="Wrong command or module name: foo.bar.baz"):
            dynamic_resolver.resolve('foo.bar.baz')

        with pytest.raises(ValueError, match="Module foo not found"):
            dynamic_resolver.resolve('foo.bar')

        with pytest.raises(ValueError, match="Command foo not found in module core"):
            dynamic_resolver.resolve('core.foo')

        # Test that policy fallback correctly
        static_resolver = StaticPolicyResolver()
        with_fallback_dynamic_resolver = dynamic_resolver.with_fallback(static_resolver)

        assert with_fallback_dynamic_resolver.resolve('tdigest.min').request_policy == RequestPolicy.DEFAULT_KEYED
        assert with_fallback_dynamic_resolver.resolve('tdigest.min').response_policy == ResponsePolicy.DEFAULT_KEYED

        # Extended chain with one more resolver
        mock_command_parser = Mock(spec=CommandsParser)
        foo_bar_policy = CommandPolicies(request_policy=RequestPolicy.DEFAULT_KEYLESS, response_policy=ResponsePolicy.DEFAULT_KEYLESS)

        mock_command_parser.get_command_policies.return_value = {
            'foo': {
                'bar': foo_bar_policy,
            }
        }
        another_dynamic_resolver = DynamicPolicyResolver(mock_command_parser)
        with_fallback_static_resolver = static_resolver.with_fallback(another_dynamic_resolver)
        with_double_fallback_dynamic_resolver = dynamic_resolver.with_fallback(with_fallback_static_resolver)

        assert with_double_fallback_dynamic_resolver.resolve('foo.bar') == foo_bar_policy