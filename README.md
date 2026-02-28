# xhsfwbot

![](./res/desc.jpg)

A bot that forward REDNote to Telegram Message or Telegraph

[![Require: Python 3.13](https://img.shields.io/badge/Python-3.13-blue?logo=python)](https://www.python.org/)

[![Require: mitmproxy 12.1.2](https://img.shields.io/badge/mitmproxy-12.1.2-blue)](https://pypi.org/project/mitmproxy/)
[![Require: Telethon](https://img.shields.io/badge/Telethon-MTProto-blue)](https://pypi.org/project/telethon/)
[![Require: telegraph 2.2.0](https://img.shields.io/badge/telegraph-2.2.0-blue)](https://pypi.org/project/telegraph/)
[![Require: Flask 3.1.2](https://img.shields.io/badge/Flask-3.1.2-blue)](https://pypi.org/project/Flask/)
[![Require: pytz 2025.2](https://img.shields.io/badge/pytz-2025.2-blue)](https://pypi.org/project/pytz/)
[![Require: python-dotenv 1.1.1](https://img.shields.io/badge/python--dotenv-1.1.1-blue)](https://pypi.org/project/python-dotenv/)
[![Require: requests 2.32.5](https://img.shields.io/badge/requests-2.32.5-blue)](https://pypi.org/project/requests/)
[![Require: paramiko 4.0.0](https://img.shields.io/badge/paramiko-4.0.0-blue)](https://www.paramiko.org/)
[![Require: FFmpeg](https://shields.io/badge/FFmpeg-%23171717.svg?logo=ffmpeg&style=for-the-badge&labelColor=171717&logoColor=5cb85c)](https://ffmpeg.org)

[![xhsfw Bot](https://img.shields.io/badge/xhsfw-Bot-green?logo=telegram)](https://t.me/xhsfwbot)

## Requirements

1. Server side: Python 3.13 or newer on Linux, macOS or Windows.

2. Device side: A rooted Android device or emulator, or jailbroken iOS device, with REDNote app installed.

3. Both server and device must be in the same network, and server side should has stable access to Telegram, Telegraph server and Gemini API.

## How it works
![](./res/diagram.png)

## Deployment guide

### Server side
Download [platform-tools](https://developer.android.com/tools/releases/platform-tools) and add `adb` to `PATH` if you are using an Android device.

```bash
sudo apt install libzbar-dev ffmpeg
git clone https://github.com/francgossin/xhsfwbot.git
cd xhsfwbot
mkdir -p data
mkdir -p log
python3.13 -m venv .venv
```
Create a `.env` file with your own configuration.
```python
BOT_TOKEN="TelegramBotToken"
ADMIN_ID=1234567890
API_ID=12345678
API_HASH="your_api_hash_from_my.telegram.org"

# 0: Android with root; 1: Jailbroken iOS
TARGET_DEVICE_TYPE=0

FLASK_SERVER_NAME=example.com
FLASK_SERVER_PORT=6789

# optional: socks5://127.0.0.1:7890 or http://127.0.0.1:7890
TELEGRAM_PROXY=

BARK_TOKEN=barktoken
BARK_KEY=barkkey
BARK_IV=123

GEMINI_API_KEY=AIGEMINISERVICE

CHANNEL_ID=-1234567890
```

Run xhsfwbot.py, the network must have a stable access to Telegram and Gemini.
```bash
# export https_proxy when necessary.
python xhsfwbot.py
```

Run these two scripts on a local computer with Android phone / emulator / iOS device.
```bash
python mitm_server.py
```

```bash
python shared_server.py
```
### Device side

Start `mitm_server.py` and set device proxy on Wi-Fi settings.

Host: server side IP address.

Port: mitm server's port number.

Open [mitm.it](http://mitm.it) on device browser and follow the corresponding guide.

#### iOS devices

Check [ios.cfw.guide](https://ios.cfw.guide/) to jailbreak your iOS device. 

Then install `OpenSSH` through APT package manager for jailbroken iOS like Cydia or Sileo.

#### Android devices
Guide of [Android Emulator](https://docs.mitmproxy.org/stable/howto/install-system-trusted-ca-android/)

If you are not using an emulator, you need to root your device and then manually move CA certificate to system partition, or use [Magisk](https://docs.mitmproxy.org/stable/howto/install-system-trusted-ca-android/#instructions-when-using-magisk) method, and then reboot.
