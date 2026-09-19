import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run slow integration tests that require Docker and real network access to digimon.net",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    skip_integration = pytest.mark.skip(reason="needs --run-integration (spins up Docker, hits the real site)")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
