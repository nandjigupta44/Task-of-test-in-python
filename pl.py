import requests
import json
from urllib.parse import urljoin
from db_config import pl_table

BASE_URL = "https://www.sunglasshut.com/ca-en/"

url = "https://21ogkm5th5-dsn.algolia.net/1/indexes/prod_live_sgh_en-ca__grouped/query"

categories = {
    "men": "/mens-sunglasses",
    "women": "/womens-sunglasses"
}

for gender, path in categories.items():
    page = 0
    while True:
        # Update headers dynamically with category path
        
        headers = {
            'Accept': '/',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Origin': f'https://www.sunglasshut.com{path}',
            'Referer': f'https://www.sunglasshut.com{path}',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'content-type': 'application/x-www-form-urlencoded',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'x-algolia-api-key': 'dc91173a4a5d669a3eef474e5836e94f',
            'x-algolia-application-id': '21OGKM5TH5',
        }

        # Prepare Algolia request data
        data = {
            "query": "",
            "hitsPerPage": 100,
            "page": page,
            "ruleContexts": [f"cms_hp_{gender}"],
            "userToken": "anonymous"
        }

        response = requests.post(url, headers=headers, data=json.dumps(data))
        res_json = response.json()
        hits = res_json.get("hits", [])

        if not hits:
            break

        for item in hits:
            raw_url = item.get("url") or item.get("productPage")
            product_id = (
                item.get("parentProductId") or
                item.get("masterProductId") or
                item.get("productId") or
                item.get("objectID")
            )

            if not raw_url:
                continue

            full_url = urljoin(BASE_URL, raw_url.lstrip("/"))

            # Insert into MongoDB with gender
            result = pl_table.update_one(
                {"url": full_url},
                {"$setOnInsert": {
                    "product_id": product_id,
                    "gender": gender,
                    "status": "pending"      # default status
                }},
                upsert=True
            )

            print(f"URL: {full_url} | Product ID: {product_id} | Gender: {gender} | {'Inserted' if result.upserted_id else 'Exists'}")

        page += 1
