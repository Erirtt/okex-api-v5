import ccxt

okcoin = ccxt.okcoin()
markets = okcoin.load_markets()
print(okcoin.id, markets)