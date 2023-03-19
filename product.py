# MongoDB connectie
from pymongo import MongoClient
# Voor de relationele database + PostgreSQL
import psycopg2
import pandas as pd

# Maak de connectie met Mongo
client = MongoClient()
print("De producten in MongoDB worden aangewezen met een cursor")


cursor = client.Database.tess.find()

# Producten opslaan met list comprehension
producten = [product for product in cursor]
print(producten[0])





# Lees de data uit

# Zet de data over

conn = psycopg2.connect(
    database = "HUwebshop4",
    host = 'localhost',
    port = '5432',
    password = 'Bommel2011',
    user = 'postgres'
)

conn.autocommit = True
cursor = conn.cursor()


for product in producten:
    id = product.get('_id')
    naam = product.get('name')

    prijs = product.get('price')
    if prijs is not None:
        verkoopprijs = prijs.get('selling_price')
    else:
        verkoopprijs = 0

    gender = product.get('gender')
    merk = product.get('brand')
    herhaalaankoop = product.get('herhaalaankopen')
    categorie = product.get('category')
    subcat1 = product.get('sub_category')
    subcat2 = product.get('sub_sub_category')
    subcat3 = product.get('sub_sub_sub_category')
    stock = product.get('properties')
    if stock is not None:
        stock_level = stock.get('stock')
    else:
        stock_level = 0

    print(id)






    # Hier geef ik variabelen die ik daaronder door middel van execute naar de database schrijf
    product_database = 'INSERT INTO product(id, brand, category, gender, sub_category, sub_sub_category, sub_sub_sub_category, repeat_purchases, price) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
    # stock_database = 'INSERT INTO stock(stock_level) VALUES(%s)'
    cursor.execute(product_database, (id, merk, categorie, gender, subcat1, subcat2, subcat3, herhaalaankoop, verkoopprijs/100))
    # cursor.execute(stock_database, (stock_level))








