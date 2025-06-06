import os
import json
import requests
import pandas as pd
from tqdm import tqdm
from dj_ref import DJ_REFS


BEARER = os.environ["WATCHUP_BEARER"]
SESSION_ID = os.environ["WATCHUP_SESSION_ID"]


# this needs a fullset
REFS = [
	# "126710GRNR-0003",
	# "126500LN-0001",
	# "5968A-001", 
	# "278271-0006", 
] + DJ_REFS

results = {}
for ref in tqdm(REFS):
	res = requests.get(f'https://api.watchup.ai/watches/{ref}/messages/', headers={'Authorization': f'Bearer {BEARER}', 'Session-Id': SESSION_ID})
	if res.status_code != 200:
		print(f"Ref: {ref} parsing error")

	# x has 3 keys ['new', 'used', 'looking']
	x = json.loads(res.text) 

	results[ref] = x


# simple data analysis

dfs = []
for ref, data in results.items():
	df = pd.DataFrame(data['new'])
	df['ref'] = ref
	dfs.append(df)

df = pd.concat(dfs).reset_index(drop=True)

df['seller_status'] =  df['seller'].apply(lambda x: x['grade'])
df['lowest_price_usd'] = df['lowest_prices'].apply(lambda x: x['USD'])
df['lowest_price_usdt'] = df['lowest_prices'].apply(lambda x: x['USDT'])
df['lowest_price_hkd'] = df['lowest_prices'].apply(lambda x: x['HKD'])
df['date'] = pd.to_datetime(df['sent_at']).apply(lambda x: x.date())
df['seller_contact'] =  df['seller'].apply(lambda x: x['contact'])

# df.sort_values(['date','lowest_price_hkd'],ascending=[0,1]).head(10)

df.to_csv(f"~/Downloads/dj_results.csv")
