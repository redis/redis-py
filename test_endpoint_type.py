#!/usr/bin/env python3
"""
Quick test script to verify the MaintenanceEventsConfig endpoint type functionality.
"""

import sys
import os

# Add the redis module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from redis.maintenance_events import MaintenanceEventsConfig, EndpointType

def test_endpoint_type_constants():
    """Test that the EndpointType constants are correct."""
    print("Testing EndpointType constants...")
    
    assert EndpointType.INTERNAL_IP == "internal-ip"
    assert EndpointType.INTERNAL_FQDN == "internal-fqdn"
    assert EndpointType.EXTERNAL_IP == "external-ip"
    assert EndpointType.EXTERNAL_FQDN == "external-fqdn"
    assert EndpointType.NONE == "none"
    
    valid_types = EndpointType.get_valid_types()
    expected_types = {"internal-ip", "internal-fqdn", "external-ip", "external-fqdn", "none"}
    assert valid_types == expected_types
    
    print("✓ EndpointType constants are correct")

def test_config_validation():
    """Test that MaintenanceEventsConfig validates endpoint_type correctly."""
    print("Testing MaintenanceEventsConfig validation...")
    
    # Valid endpoint types should work
    for endpoint_type in EndpointType.get_valid_types():
        config = MaintenanceEventsConfig(endpoint_type=endpoint_type)
        assert config.endpoint_type == endpoint_type
    
    # Invalid endpoint type should raise ValueError
    try:
        MaintenanceEventsConfig(endpoint_type="invalid-type")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Invalid endpoint_type" in str(e)
    
    # None should be allowed
    config = MaintenanceEventsConfig(endpoint_type=None)
    assert config.endpoint_type is None
    
    print("✓ MaintenanceEventsConfig validation works correctly")

def test_endpoint_type_detection():
    """Test the get_endpoint_type method with various inputs."""
    print("Testing endpoint type detection...")
    
    config = MaintenanceEventsConfig()
    
    # Test IPv4 addresses
    assert config.get_endpoint_type("192.168.1.1") == EndpointType.INTERNAL_IP  # Private IPv4
    assert config.get_endpoint_type("10.0.0.1") == EndpointType.INTERNAL_IP     # Private IPv4
    assert config.get_endpoint_type("172.16.0.1") == EndpointType.INTERNAL_IP   # Private IPv4
    assert config.get_endpoint_type("8.8.8.8") == EndpointType.EXTERNAL_IP      # Public IPv4
    assert config.get_endpoint_type("1.1.1.1") == EndpointType.EXTERNAL_IP      # Public IPv4
    
    # Test IPv6 addresses
    result1 = config.get_endpoint_type("::1")
    print(f"::1 -> {result1} (expected: {EndpointType.INTERNAL_IP})")
    assert result1 == EndpointType.INTERNAL_IP           # Loopback IPv6

    result2 = config.get_endpoint_type("fd00::1")
    print(f"fd00::1 -> {result2} (expected: {EndpointType.INTERNAL_IP})")
    assert result2 == EndpointType.INTERNAL_IP       # Private IPv6

    result3 = config.get_endpoint_type("2001:db8::1")
    print(f"2001:db8::1 -> {result3} (expected: {EndpointType.EXTERNAL_IP})")
    assert result3 == EndpointType.EXTERNAL_IP   # Public IPv6
    
    # Test FQDNs without TLS
    assert config.get_endpoint_type("localhost") == EndpointType.INTERNAL_FQDN   # Single label
    assert config.get_endpoint_type("server.local") == EndpointType.INTERNAL_FQDN # .local domain
    assert config.get_endpoint_type("app.internal") == EndpointType.INTERNAL_FQDN # .internal domain
    assert config.get_endpoint_type("example.com") == EndpointType.EXTERNAL_FQDN  # Public domain
    
    # Test FQDNs with TLS
    assert config.get_endpoint_type("server.local", tls_enabled=True) == EndpointType.INTERNAL_FQDN
    assert config.get_endpoint_type("example.com", tls_enabled=True) == EndpointType.EXTERNAL_FQDN
    
    print("✓ Endpoint type detection works correctly")

def test_override_behavior():
    """Test that explicit endpoint_type overrides automatic detection."""
    print("Testing override behavior...")
    
    config = MaintenanceEventsConfig(endpoint_type=EndpointType.NONE)
    
    # Should always return the override value regardless of host
    assert config.get_endpoint_type("192.168.1.1") == EndpointType.NONE
    assert config.get_endpoint_type("8.8.8.8") == EndpointType.NONE
    assert config.get_endpoint_type("example.com") == EndpointType.NONE
    assert config.get_endpoint_type("localhost") == EndpointType.NONE
    
    print("✓ Override behavior works correctly")

def main():
    """Run all tests."""
    print("Running MaintenanceEventsConfig endpoint type tests...\n")
    
    try:
        test_endpoint_type_constants()
        test_config_validation()
        test_endpoint_type_detection()
        test_override_behavior()
        
        print("\n🎉 All tests passed!")
        return 0
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
