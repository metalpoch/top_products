from datetime import datetime
from src import fetch
from src.database import Database

URL = "https://marketplace.walmartapis.com/v3/utilities/taxonomy/departments"


def __department_products_list(departments) -> list:
    rows = []
    for iter_data in departments:
        number, name = iter_data["superDepartment"].split(" ", 1)
        super_id = int(iter_data["superDepartmentId"])
        departments = iter_data["departments"]

        get_row = lambda x: rows.append(
            {
                "id": int(x["departmentId"]),
                "department": x["departmentName"],
                "super_department_number": int(number),
                "super_department_name": name,
                "super_departments_id": super_id,
                "last_update": datetime.now(),
            }
        )

        list(map(get_row, departments))

    return rows


def update(db: Database) -> None:
    response = fetch.get(URL)
    if response.status_code != 200:
        print(response)
    else:
        response = response.json()["payload"]
        prodcuts_list = __department_products_list(response)
        db.insert_or_replace_many(table="departments", data=prodcuts_list)
