import asyncio
import yfinance as yf

stock_list = [
    "VCB", "BID", "FPT", "HPG", "CTG", "VHM", "TCB", "VPB", "VNM", "MBB",
    "GAS", "ACB", "MSN", "GVR", "LPB", "SSB", "STB", "VIB", "MWG", "HDB",
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "LLY", "AVGO",
    "JPM", "V", "UNH", "WMT", "MA", "XOM", "JNJ", "PG", "HD", "COST"
    ]

# define your message callback
def message_handler(message):
    print("Received message:", message)

async def main():
    # =======================
    # With Context Manager
    # =======================
    async with yf.AsyncWebSocket() as ws:
        await ws.subscribe(stock_list)
        await ws.listen()

    # =======================
    # Without Context Manager
    # =======================
    ws = yf.AsyncWebSocket()
    await ws.subscribe(stock_list)
    await ws.listen()

asyncio.run(main())