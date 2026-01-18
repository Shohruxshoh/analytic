#!/bin/sh
set -e

echo "======================================"
echo "🧪 API TEST START"
echo "======================================"
python api_test.py

echo ""
echo "======================================"
echo "🔥 KAFKA LOAD TEST START"
echo "======================================"
python kafka_load_producer.py

echo ""
echo "======================================"
echo "✅ ALL TESTS PASSED"
echo "======================================"
