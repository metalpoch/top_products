from os import path
from src import departments, top_products  # , wfs_inventory
from src.database import Database

BASE_PATH = path.dirname(path.abspath(__name__))
database = Database(BASE_PATH)


def main():
    print("Updating table 'departments'...")
    departments.update(db=database)
    print("Updating table 'top_products'...")
    top_products.update(db=database)
    # print("Updating table 'wfs_inventory'...")
    # wfs_inventory.update(db=database)


if __name__ == "__main__":
    main()
