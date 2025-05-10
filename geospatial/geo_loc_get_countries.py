#%%
import requests
import pandas as pd

url = "https://geo-location-data1.p.rapidapi.com/geo/get-countries"

querystring = {"pageSize":"20","page":"1"}

headers = {
	"x-rapidapi-key": "e7cbee4ae1mshd0704b6dcc65548p1632e3jsncb04d537a049",
	"x-rapidapi-host": "geo-location-data1.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())
# %%
df = pd.DataFrame(response.json()['data'])
df.head()
# %%
