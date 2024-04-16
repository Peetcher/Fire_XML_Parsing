import pandas as pd
import xml.etree.ElementTree as ET
import psycopg2
from datetime import datetime


def argument_handler(integer, divide_integer):
    if integer == 0:
        return integer
    else:
        return integer / divide_integer


# конвертация формата координат dmc в dd
def convert_dmc_to_dd(string):
    string = string.split(",")
    array_of_integers = [int(integer) for integer in string]

    decimal_degrees = array_of_integers[0] + argument_handler(array_of_integers[1], 60) + argument_handler(
        array_of_integers[2], 3.600)
    return decimal_degrees


def convert_to_integer(string: str, column_name):
    columns_with_delimeter = ["longitude", "latitude", 'monitoring_area']

    if any(column in column_name for column in columns_with_delimeter):
        string = string.replace("\xa0", ",")
        # Если встретиться формат dmc координат
        if string.count(",") == 2:
            return convert_dmc_to_dd(string)
        else:
            string = string.replace(",", ".")
    try:
        integer = int(string)
        return integer
    except ValueError:
        try:
            string_float = string.replace(",", ".")
            integer = float(string_float.replace(' ', ''))
            return integer
        except ValueError:
            return string

#
# def check_types(inserted_values):
#     #types = []
def create_insert_sql(item, table_name, check_inserted_types=False):
    keylist = list(item.attrib.keys())
    columns_name = ", ".join(keylist)
    inserted_values = []

    # формируем массив вставляемых значений
    for name_column in keylist:
        if "date" in name_column:
            inserted_values.append(datetime.strptime(item.attrib[name_column], "%d.%m.%Y %H:%M:%S"))
        else:
            inserted_values.append(convert_to_integer(item.attrib[name_column], name_column))

    # количество вставляемых аргументов
    part_of_query = "%s" + " ,%s" * (len(inserted_values) - 1)
    # формируем запрос sql
    # if check_inserted_types:
    #     check_types(inserted_values)
    # try:
    cur.execute(f"INSERT INTO {table_name} ({columns_name}) VALUES ({part_of_query})",
                  tuple(inserted_values))
    # except :
    #     print(f"строка не вставлена {inserted_values}")


def parse_xml_books(root_name, tables_names):
    for item in root.findall(root_name):
        for table_name in tables_names:
            for item_inner in item.findall(table_name):
                for item_ in item_inner.findall('item'):
                    create_insert_sql(item_, table_name)


def parse_xml_fires(root_name, item_name):
    for item in root.findall(root_name):
        for item_ in item.findall(item_name):
            create_insert_sql(item_, root_name, True)


conn = psycopg2.connect("dbname=Fires user=postgres password=0192 host=localhost")
cur = conn.cursor()

tree = ET.parse(r"C:\Users\molos\PycharmProjects\pythonProject\2016 fires.xml")
root = tree.getroot()

# колонки таблиц-справочников
table_names_books = ["air_divisions", "application_zones", "departments", "detection_ways", "divisional_forestries",
                     "fire_causes", "fire_extinguisher", "fire_kinds", "fire_states", "forest_managers", "forestries",
                     "municipalities", "regions"]

table_names_data = ['fires']

parse_xml_books('refbooks', table_names_books)
parse_xml_fires('fires', "fire")

conn.commit()
cur.close()
conn.close()
