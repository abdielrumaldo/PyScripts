import asyncio
from pathlib import Path
from pyppeteer import launch
import hashlib
import base64
import sys

class screenshot:

    def __init__(self, url):

        # Todo: Add URL validation and error handling

        self.url = self.getHash(url)

        asyncio.get_event_loop().run_until_complete(self._capture(url))

        self.path = f'{Path().absolute()}\{self.url}'


    async def _capture(self, url):
        browser = await launch(headless=True, ignoreHTTPSErrors=True, dumpio=True)
        page = await browser.newPage()
        page.setDefaultNavigationTimeout(timeout=5000)

        # Future feature to block ads/scripts on the page
        #await page.setRequestInterception(True)
        #page.on('request', lambda req: asyncio.ensure_future(_intercept(req)))

        await page.goto(url)
        await page.screenshot({'path': f'{self.url}.png', 'fullPage': True})
    
        await browser.close()


    def getHash(self, url):
        # We created this function to quickly generate a hash for lookup

        temp = hashlib.md5(str.encode(url)).digest()
        hashedUrl = base64.urlsafe_b64encode(temp)
        return hashedUrl.decode()

    def getPath(self):

         return self.path

    async def _intercept(self, request):
        '''Skip the loading of scripts in order to prevent ads/videos from showing up'''
        if any(request.resourceType == _ for _ in ('stylesheet image script')):
            await request.abort()
        else:
            await request.continue_()


if __name__ == "__main__":

    try:
        arg = sys.argv[1]
    except IndexError:
        raise SystemExit(f"Usage: Give this program a valid URL and I will generate a screenshot")
    code = screenshot(arg)
    print(code.getPath())
    