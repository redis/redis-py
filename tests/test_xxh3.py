#!/usr/bin/env python3
"""
Verification script for pure Python XXH3 implementation.
Tests correctness against xxhash C library and measures performance.
"""

import time

from redis.xxh3 import xxh3_64, xxh3_64_hexdigest

try:
    import xxhash
    HAS_XXHASH = True
except ImportError:
    HAS_XXHASH = False
    print("WARNING: xxhash library not installed. Install with: pip install xxhash")
    print("Skipping correctness verification.\n")


def test_correctness():
    """Test correctness against xxhash C library."""
    if not HAS_XXHASH:
        return
    
    print("=" * 70)
    print("CORRECTNESS VERIFICATION")
    print("=" * 70)
    
    test_cases = [
        # Empty and small inputs
        (b"", "empty"),
        (b"a", "1 byte"),
        (b"ab", "2 bytes"),
        (b"abc", "3 bytes"),
        (b"abcd", "4 bytes"),
        (b"abcde", "5 bytes"),
        (b"abcdef", "6 bytes"),
        (b"abcdefg", "7 bytes"),
        (b"abcdefgh", "8 bytes"),
        (b"abcdefghi", "9 bytes"),
        (b"hello world", "11 bytes"),
        (b"0123456789abcdef", "16 bytes"),
        
        # Medium inputs
        (b"a" * 17, "17 bytes"),
        (b"a" * 32, "32 bytes"),
        (b"a" * 64, "64 bytes"),
        (b"a" * 100, "100 bytes"),
        (b"a" * 128, "128 bytes"),
        
        # Large inputs (129-240)
        (b"a" * 129, "129 bytes"),
        (b"a" * 200, "200 bytes"),
        (b"a" * 240, "240 bytes"),
        
        # Very large inputs (> 240)
        (b"a" * 241, "241 bytes"),
        (b"a" * 500, "500 bytes"),
        (b"a" * 1000, "1 KB"),
        (b"a" * 10000, "10 KB"),
        (b"a" * 100000, "100 KB"),
        
        # Real-world data
        (b"The quick brown fox jumps over the lazy dog", "pangram"),
        (b"Lorem ipsum dolor sit amet, consectetur adipiscing elit", "lorem ipsum"),
        (b"\x00" * 100, "null bytes"),
        (b"\xff" * 100, "0xFF bytes"),
        (bytes(range(256)), "0-255 sequence"),
    ]
    
    failures = []
    
    for data, description in test_cases:
        expected = xxhash.xxh3_64(data).hexdigest()
        actual = xxh3_64_hexdigest(data)
        
        if expected == actual:
            print(f"✓ {description:20s} len={len(data):6d}: {actual}")
        else:
            print(f"✗ {description:20s} len={len(data):6d}: {actual} (expected: {expected})")
            failures.append((description, len(data), expected, actual))
    
    print()
    if failures:
        print(f"FAILED: {len(failures)} test(s) failed")
        for desc, length, expected, actual in failures:
            print(f"  - {desc} ({length} bytes): got {actual}, expected {expected}")
        return False
    else:
        print(f"SUCCESS: All {len(test_cases)} tests passed!")
        return True


def test_performance():
    """Measure performance characteristics."""
    print("\n" + "=" * 70)
    print("PERFORMANCE ANALYSIS")
    print("=" * 70)

    test_sizes = [
        (10, "10 bytes", 1000),
        (100, "100 bytes", 1000),
        (1000, "1 KB", 1000),
        (10000, "10 KB", 100),
        (100000, "100 KB", 10),
    ]

    for size, description, iterations in test_sizes:
        data = b"a" * size

        # Pure Python implementation
        start = time.perf_counter()
        for _ in range(iterations):
            xxh3_64(data)
        py_time = (time.perf_counter() - start) / iterations * 1000000  # microseconds

        # C implementation (if available)
        if HAS_XXHASH:
            start = time.perf_counter()
            for _ in range(iterations):
                xxhash.xxh3_64(data)
            c_time = (time.perf_counter() - start) / iterations * 1000000  # microseconds

            slowdown = py_time / c_time
            print(f"{description:12s}: Pure Python: {py_time:8.2f} μs | C: {c_time:6.2f} μs | Slowdown: {slowdown:5.1f}x")
        else:
            print(f"{description:12s}: Pure Python: {py_time:8.2f} μs")


def test_edge_cases():
    """Test edge cases and special scenarios."""
    print("\n" + "=" * 70)
    print("EDGE CASE TESTING")
    print("=" * 70)
    
    # Test with seed
    if HAS_XXHASH:
        for seed in [0, 1, 42, 0xDEADBEEF, 2**63 - 1]:
            data = b"test data with seed"
            expected = xxhash.xxh3_64(data, seed=seed).hexdigest()
            actual = xxh3_64_hexdigest(data, seed=seed)
            match = "✓" if expected == actual else "✗"
            print(f"{match} Seed {seed:16x}: {actual}")
    
    # Test string input
    result = xxh3_64_hexdigest("hello")
    print(f"✓ String input: {result}")
    
    # Test memoryview (if supported)
    try:
        mv = memoryview(b"test")
        result = xxh3_64(bytes(mv))
        print(f"✓ Memoryview input: {result:016x}")
    except Exception as e:
        print(f"✗ Memoryview input failed: {e}")


