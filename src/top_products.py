import time
from datetime import datetime
from tqdm import tqdm
from src import fetch
from src.database import Database


URL = f"https://marketplace.walmartapis.com/v3/insights/items/trending"


def top_products_list(products: list, department_id: int) -> list:
    rows = []
    for product in products:
        rows.append(
            {
                "id": product["itemId"],
                "department_id": department_id,
                "category_name": product["categoryName"],
                "sub_category_name": product["subCategoryName"],
                "product_name": product["productName"],
                "is_two_day_eligible": product["isTwoDayEligible"],
                "total_offers": product["totalOffers"],
                "isbn": product["isbn"],
                "issn": product["issn"],
                "exists_for_seller": product["existsForSeller"],
                "rank": product["rank"],
                "brand": product["brand"],
                "last_update": datetime.now(),
            }
        )

    return rows


def update(db: Database) -> None:
    token = fetch.auth_token()
    idx = db.get("departments", ["id"])
    i, total_idx = 0, len(idx)
    progress_bar = tqdm(total=total_idx)
    while i < total_idx:
        url = f"{URL}?departmentId={idx[i]}&limit=10000"
        response = fetch.get(url, token=token)

        if response.status_code == 200:
            data_json = response.json()["payload"]["items"]
            try:
                db.insert_or_replace_many(
                    table="top_products", data=top_products_list(data_json, idx[i])
                )
            except BaseException as e:
                pass
                # print("⚠️ ", {"error": e, "departmentId": idx[i]})
            finally:
                i += 1
                progress_bar.update(1)
                continue

        # Requests Errors
        if response.status_code == 401:
            token = fetch.auth_token()
            # print("⚠️ token reloaded...")

        elif response.status_code == 429:
            timestamp = int(response.headers["X-Next-Replenishment-Time"])
            now = time.time() * 1000
            timeout = (timestamp - now) / 1000
            if timeout > 0:
                # print(f"⚠️ waiting {round(timeout, 1)} seconds...")
                time.sleep(timeout)
