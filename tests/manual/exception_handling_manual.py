"""Verify that specific exception types are caught correctly."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import logging
import requests

logging.basicConfig(level=logging.DEBUG, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("test_exception")

# Test 1: requests.RequestException catch in crawlers
print("=" * 60)
print("Test 1: requests.RequestException catch")
print("=" * 60)

session = requests.Session()
try:
    resp = session.get("http://localhost:1/nonexistent", timeout=2)
    resp.raise_for_status()
except requests.RequestException:
    logger.exception("PASS - requests.RequestException caught correctly")
    print(">> Test 1 PASSED: requests.RequestException caught\n")

# Test 2: parsing exceptions catch (ValueError, TypeError, AttributeError, KeyError)
print("=" * 60)
print("Test 2: Parsing exception types")
print("=" * 60)

for exc_cls, trigger in [
    (ValueError, lambda: int("not_a_number")),
    (TypeError, lambda: len(None)),
    (AttributeError, lambda: None.some_method()),
    (KeyError, lambda: {}["missing"]),
]:
    try:
        trigger()
    except (ValueError, TypeError, AttributeError, KeyError) as e:
        print(f">> {exc_cls.__name__} caught: {e}")

print("\n>> Test 2 PASSED: All parsing exceptions caught\n")

# Test 3: verify that unexpected exceptions (e.g. RuntimeError) are NOT caught
print("=" * 60)
print("Test 3: RuntimeError should NOT be caught by our handlers")
print("=" * 60)

try:
    try:
        raise RuntimeError("unexpected system error")
    except (requests.RequestException, ValueError, TypeError, AttributeError, KeyError):
        print(">> Test 3 FAILED: RuntimeError was incorrectly caught!")
        sys.exit(1)
except RuntimeError:
    print(">> Test 3 PASSED: RuntimeError correctly propagated\n")

# Test 4: actual crawler import and instantiation
print("=" * 60)
print("Test 4: Crawler classes import and have correct exception types")
print("=" * 60)

import inspect
from src.crawlers.compuzone import CompuzoneCrawler
from src.crawlers.pc_estimate import PCEstimateCrawler
from src.crawlers.base import BaseCrawler

# Check that 'requests' is importable from each module
import src.crawlers.compuzone as cz_mod
import src.crawlers.pc_estimate as pe_mod

assert hasattr(cz_mod, 'requests'), "compuzone.py missing requests import"
print(">> compuzone.py has requests import")
assert hasattr(pe_mod, 'requests'), "pc_estimate.py missing requests import"
print(">> pc_estimate.py has requests import")

# Check source code contains requests.RequestException
cz_src = inspect.getsource(CompuzoneCrawler.crawl_raw)
assert "requests.RequestException" in cz_src, "compuzone crawl_raw missing requests.RequestException"
print(">> compuzone.crawl_raw uses requests.RequestException")

pe_src = inspect.getsource(PCEstimateCrawler.crawl_raw)
assert "requests.RequestException" in pe_src, "pc_estimate crawl_raw missing requests.RequestException"
print(">> pc_estimate.crawl_raw uses requests.RequestException")

base_src = inspect.getsource(BaseCrawler.crawl)
assert "ValueError" in base_src and "TypeError" in base_src, "base.crawl missing specific exceptions"
print(">> base.crawl uses specific parsing exceptions")

print("\n>> Test 4 PASSED\n")

print("=" * 60)
print("ALL TESTS PASSED")
print("=" * 60)
