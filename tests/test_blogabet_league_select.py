import asyncio
from pathlib import Path

import pytest
from playwright.async_api import async_playwright

from blogabet_publisher import select_league_by_tournament


def test_select_league_from_static_blogabet_html() -> None:
    html_path = Path(__file__).resolve().parents[1] / "blogbet.html"
    html_content = html_path.read_text(encoding="utf-8")

    async def _run() -> None:
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
            except Exception as exc:  # noqa: BLE001
                pytest.skip(f"Playwright browser unavailable: {exc}")

            page = await browser.new_page()
            try:
                await page.set_content(html_content, wait_until="domcontentloaded")
                result = await select_league_by_tournament(
                    page,
                    "Algeria - Professional Ligue 1",
                    "goals",
                )
                assert result["best"]["title"] == "algeria professional ligue 1"

                corners_result = await select_league_by_tournament(
                    page,
                    "Algeria - Ligue 1",
                    "corners",
                )
                assert "corn" in corners_result["best"]["title"].lower()
            finally:
                await page.close()
                await browser.close()

    asyncio.run(_run())
