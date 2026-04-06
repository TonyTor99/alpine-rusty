import asyncio
import os
from pathlib import Path

import pytest

from blogabet_publisher import BlogabetConfig, BlogabetPublisher


@pytest.mark.manual
def test_blogabet_manual_session_check() -> None:
    if os.getenv("BLOGABET_E2E", "0") != "1":
        pytest.skip("Set BLOGABET_E2E=1 for manual Blogabet checks")

    storage_state_path = os.getenv("BLOGABET_STORAGE_STATE_PATH", "./blogabet_state.json")
    path = Path(storage_state_path)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / path

    assert path.exists(), "Storage state file is required for manual Blogabet E2E"

    cfg = BlogabetConfig(
        enabled=True,
        storage_state_path=str(path),
        headless=True,
        default_stake=3,
        admin_tg_chat_id="",
    )
    async def _run() -> None:
        publisher = BlogabetPublisher(cfg, logger=None)
        try:
            await publisher.ensure_session()
        finally:
            await publisher.close()

    asyncio.run(_run())
