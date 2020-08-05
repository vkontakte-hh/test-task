import requests

date = "2020-07-30"
access_key = "c99033bf278986db036c4344d9d40f4a"
symbols = ["USD", "AUD", "CAD", "PLN", "MXN"]
URL = f"http://data.fixer.io/api/"
data = requests.get(URL + date, params={"symbols": ','.join(symbols), "access_key": access_key})
data = data.json()
print(data)
