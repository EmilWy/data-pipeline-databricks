'''
=============================================================
Generate data for customers table
=============================================================
Script Purpose:
    - Generate artificial data for customers table
    - Create a Spark DataFrame with the following columns: 
        customer_id, 
        customer_name, 
        customer_type  
        location
=============================================================
 '''

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

customers = [
    (6, "Marek Wisniewski", "INDIVIDUAL", "Lodz"),
    (7, "Katarzyna Wojcik", "INDIVIDUAL", "Warsaw"),
    (8, "Tomasz Kaminski", "INDIVIDUAL", "Krakow"),
    (9, "Malgorzata Lewandowska", "INDIVIDUAL", "Wroclaw"),
    (10, "Pawel Zielinski", "INDIVIDUAL", "Poznan"),
    (11, "Agnieszka Szymanska", "INDIVIDUAL", "Gdansk"),
    (12, "Michal Dabrowski", "INDIVIDUAL", "Katowice"),
    (13, "Joanna Kozlowska", "INDIVIDUAL", "Lublin"),
    (14, "Robert Jankowski", "INDIVIDUAL", "Szczecin"),
    (15, "Ewa Mazur", "INDIVIDUAL", "Bialystok"),
    (16, "Krzysztof Krawczyk", "INDIVIDUAL", "Warsaw"),
    (17, "Barbara Piotrowska", "INDIVIDUAL", "Krakow"),
    (18, "Marcin Grabowski", "INDIVIDUAL", "Wroclaw"),
    (19, "Monika Pawlowska", "INDIVIDUAL", "Poznan"),
    (20, "Lukasz Michalski", "INDIVIDUAL", "Gdansk"),
    (21, "Natalia Nowicka", "INDIVIDUAL", "Lodz"),
    (22, "Adam Zalewski", "INDIVIDUAL", "Warsaw"),
    (23, "Karolina Wieczorek", "INDIVIDUAL", "Krakow"),
    (24, "Grzegorz Wroblewski", "INDIVIDUAL", "Katowice"),
    (25, "Aleksandra Sikora", "INDIVIDUAL", "Lublin"),
    (26, "Marek Wrona", "BUSINESS", "Warsaw"),
    (27, "PolTech Sp. z o.o.", "BUSINESS", "Krakow"),
    (28, "Green Logistics Sp. z o.o.", "BUSINESS", "Wroclaw"),
    (29, "Nova Solutions S.A.", "BUSINESS", "Poznan"),
    (30, "Baltic Trade Sp. z o.o.", "BUSINESS", "Gdansk"),
    (31, "TechVision Sp. z o.o.", "BUSINESS", "Lodz"),
    (32, "Future Systems S.A.", "BUSINESS", "Warsaw"),
    (33, "ProOffice Sp. z o.o.", "BUSINESS", "Krakow"),
    (34, "Smart Industry S.A.", "BUSINESS", "Katowice"),
    (35, "Lublin Express Sp. z o.o.", "BUSINESS", "Lublin"),
    (36, "North Logistics S.A.", "BUSINESS", "Szczecin"),
    (37, "Podlasie Trade Sp. z o.o.", "BUSINESS", "Bialystok"),
    (38, "Central Market S.A.", "BUSINESS", "Warsaw"),
    (39, "Krakow Business Group", "BUSINESS", "Krakow"),
    (40, "Wroclaw Solutions Sp. z o.o.", "BUSINESS", "Wroclaw"),
    (41, "Poznan Distribution S.A.", "BUSINESS", "Poznan"),
    (42, "Amber Logistics Sp. z o.o.", "BUSINESS", "Gdansk"),
    (43, "Lodz Manufacturing S.A.", "BUSINESS", "Lodz"),
    (44, "Capital Services Sp. z o.o.", "BUSINESS", "Warsaw"),
    (45, "Royal Consulting S.A.", "BUSINESS", "Krakow"),
    (46, "Daniel Krupa", "INDIVIDUAL", "Warsaw"),
    (47, "Sylwia Adamczyk", "INDIVIDUAL", "Wroclaw"),
    (48, "Mateusz Baran", "INDIVIDUAL", "Poznan"),
    (49, "Iwona Dudek", "INDIVIDUAL", "Gdansk"),
    (50, "Rafal Czarnecki", "INDIVIDUAL", "Lodz"),
    (51, "Patrycja Walczak", "INDIVIDUAL", "Katowice"),
    (52, "Szymon Tomaszewski", "INDIVIDUAL", "Lublin"),
    (53, "Justyna Borkowska", "INDIVIDUAL", "Szczecin"),
    (54, "Dawid Lis", "INDIVIDUAL", "Bialystok"),
    (55, "Weronika Kaczmarek", "INDIVIDUAL", "Warsaw"),
    (56, "Kamil Ostrowski", "INDIVIDUAL", "Krakow"),
    (57, "Marta Sobczak", "INDIVIDUAL", "Wroclaw"),
    (58, "Wojciech Urbanski", "INDIVIDUAL", "Poznan"),
    (59, "Dominika Malinowska", "INDIVIDUAL", "Gdansk"),
    (60, "Sebastian Gorski", "INDIVIDUAL", "Lodz"),
    (61, "Emilia Kubiak", "INDIVIDUAL", "Warsaw"),
    (62, "Artur Maciejewski", "INDIVIDUAL", "Krakow"),
    (63, "Beata Kaczmarek", "INDIVIDUAL", "Katowice"),
    (64, "Przemyslaw Kruk", "INDIVIDUAL", "Lublin"),
    (65, "Martyna Wysocka", "INDIVIDUAL", "Szczecin"),
    (66, "Digital Works Sp. z o.o.", "BUSINESS", "Warsaw"),
    (67, "Metro Distribution S.A.", "BUSINESS", "Krakow"),
    (68, "Eco Transport Sp. z o.o.", "BUSINESS", "Wroclaw"),
    (69, "West Solutions S.A.", "BUSINESS", "Poznan"),
    (70, "SeaLine Logistics Sp. z o.o.", "BUSINESS", "Gdansk"),
    (71, "Lodz Trade Group S.A.", "BUSINESS", "Lodz"),
    (72, "Mazovia Services Sp. z o.o.", "BUSINESS", "Warsaw"),
    (73, "Dragon Technologies S.A.", "BUSINESS", "Krakow"),
    (74, "Silesia Manufacturing Sp. z o.o.", "BUSINESS", "Katowice"),
    (75, "Eastern Logistics S.A.", "BUSINESS", "Lublin"),
    (76, "Pomerania Trade Sp. z o.o.", "BUSINESS", "Gdansk"),
    (77, "WestPort Services S.A.", "BUSINESS", "Szczecin"),
    (78, "Bialystok Solutions Sp. z o.o.", "BUSINESS", "Bialystok"),
    (79, "Poland Business Center S.A.", "BUSINESS", "Warsaw"),
    (80, "Krakow Digital Sp. z o.o.", "BUSINESS", "Krakow"),
    (81, "Filip Pawlak", "INDIVIDUAL", "Warsaw"),
    (82, "Oliwia Kurek", "INDIVIDUAL", "Krakow"),
    (83, "Jakub Matusiak", "INDIVIDUAL", "Wroclaw"),
    (84, "Amelia Majewska", "INDIVIDUAL", "Poznan"),
    (85, "Bartosz Janik", "INDIVIDUAL", "Gdansk"),
    (86, "Zuzanna Sokolowska", "INDIVIDUAL", "Lodz"),
    (87, "Mikolaj Marciniak", "INDIVIDUAL", "Warsaw"),
    (88, "Julia Tomaszewska", "INDIVIDUAL", "Krakow"),
    (89, "Igor Brzezinski", "INDIVIDUAL", "Wroclaw"),
    (90, "Lena Duda", "INDIVIDUAL", "Poznan"),
    (91, "Maciej Kalinowski", "INDIVIDUAL", "Gdansk"),
    (92, "Wiktoria Bielecka", "INDIVIDUAL", "Katowice"),
    (93, "Nikodem Sawicki", "INDIVIDUAL", "Lublin"),
    (94, "Laura Rutkowska", "INDIVIDUAL", "Szczecin"),
    (95, "Hubert Wilk", "INDIVIDUAL", "Bialystok"),
    (96, "Zofia Pawlik", "INDIVIDUAL", "Warsaw"),
    (97, "Antoni Bednarek", "INDIVIDUAL", "Krakow"),
    (98, "Kinga Cieslak", "INDIVIDUAL", "Wroclaw"),
    (99, "Maksymilian Sadowski", "INDIVIDUAL", "Poznan"),
    (100, "Nina Lisowska", "INDIVIDUAL", "Gdansk"),
    (101, "Tomasz Kowalski", "INDIVIDUAL", "Warsaw"),
    (102, "", "INDIVIDUAL", "Krakow"),
    (103, "Fast Delivery Sp. z o.o.", "COMPANY", "Wroclaw"),
    (104, "Marek Wisniewski", "INDIVIDUAL", "Warszawa"),
    (105, "Green Market Sp. z o.o.", "BUSINESS", "Wroclaw"),
    (106, "Anna Kowalska", "INDIVIDUAL", "Warsaw"),
    (107, "Test Customer", "BUSINESS", "Krakkow"),
    (108, "Nova Transport Sp. z o.o.", "BUSINESS", ""),
    (109, "Piotr Nowak", "UNKNOWN", "Poznan"),
    (110, "ABC Logistics Sp. z o.o.", "BUSINESS", "Gdansk")
]

columns = [
    "customer_id",
    "customer_name",
    "customer_type",
    "city"
]

df = spark.createDataFrame(customers, columns)

df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/Volumes/deliveries/default/generated_dim_data/gen_customer_info")

print("dim_customer created")