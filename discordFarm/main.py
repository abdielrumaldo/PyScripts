"""
Overview: Create a bot/deamon that writes text in discord channels in order to farm for XP points
in a server.

Challenges: We cannot use the discord bot in order to do this because you cannot tie it to you account.
This is implemented for obvious reasons, but I am not trying to do anything malicious, just win a (small) bet.

Objectives:
    1. Have the script write text without hitting the spam limits. The text must be coherent and should not be
    obnoxious. (Required)

    2. If possible pull from a specific source such as a file with movie subtitles or a book and type it into
    a dedicated channel. (Required)

    3. Auto-delete text that was pushed into discord in order to be less obnoxious.
        a. This may take some research into how the bot calculates XP
        b. "Up arrow > ctrl+a > backspace > enter enter" Shortcut to delete the previous message.

Resources:
    1. Possibly use a POST request to mimic the user. We can look at the POST request being made and copy the auth
    token in order to mimic the user:
        a. https://requests.readthedocs.io/en/master/
        b. https://www.labnol.org/code/20563-post-message-to-discord-webhooks
        c. This solution is not viable.

    2. Using Python Ctypes in order to automate writing messages
        a. https://www.thetopsites.net/article/51774514.shtml
        b. https://github.com/asweigart/pyautogui
"""

import pyautogui
import time


def write(message):

    """
    Writes specified message, then deletes the message after writing it.

    :param message:
    :return:
    """

    # replace all taps and newlines with space so that the bot can delete properly
    message = message.replace('\n', ' ')
    message = message.replace('\t', ' ')

    # Prints the message
    pyautogui.write(f'{message}', interval=0.03)

    # Keyboard shortcut to delete the previous message
    pyautogui.press('enter')
    time.sleep(.4)
    pyautogui.press('up')
    time.sleep(.4)
    pyautogui.hotkey('ctrl', 'a')
    time.sleep(.4)
    pyautogui.press('backspace')
    time.sleep(.4)
    pyautogui.press('enter')
    time.sleep(.4)
    pyautogui.press('enter')


if __name__ == '__main__':

    file = open("text.txt")

    # gives me time to move cursor
    time.sleep(2)

    empty = ""
    for line in file:
        empty += line

    # Split into sentences // Nothing we can really do about Mr. and Mrs. ... yet.
    empty = empty.split(".")

    for line in empty:
        write(line)
        # Bot only gives us points every 60 seconds
        time.sleep(60)