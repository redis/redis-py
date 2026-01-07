#!/bin/bash
set -e

case "$1" in
    base)
        pytest tests/  
    ;;
    new)
        pytest tests/new_test_sentinel_failover.py
    ;;
    *)
        echo "Usage: ./test.sh {base|new}"
        exit 1
    ;;
esac
