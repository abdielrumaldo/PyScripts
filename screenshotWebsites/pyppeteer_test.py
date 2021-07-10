import asyncio
from pyppeteer import launch

async def main():
    browser = await launch(headless=True, ignoreHTTPSErrors=True, dumpio=True)
    page = await browser.newPage()
    page.setDefaultNavigationTimeout(timeout=0)
    await page.setRequestInterception(True)
    page.on('request', lambda req: asyncio.ensure_future(intercept(req)))
    await page.goto('https://www.bbc.com/news/world-us-canada-54036637')
    await page.screenshot({'path': 'screen.png', 'fullPage': True})
    # The name of the file can be the ID of the news article. That will allow you to easily offload gathering of images to another function.
    # No need to way to add entried in the webpage
    await browser.close()

async def intercept(request):
    if any(request.resourceType == _ for _ in ('stylesheet')):
        await request.abort()
    else:
        await request.continue_()



asyncio.get_event_loop().run_until_complete(main())