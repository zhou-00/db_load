import csv, time

start_time = time.perf_counter()


csvPath = 'Buildings_with_name__age__size__accessibility__and_bicycle_facilities.csv'

with open(csvPath, 'r', encoding='utf-8-sig') as csvFile, open('derby-script.sql', 'w') as sqlFile:

  csvReader = csv.reader(csvFile)

  accessibility_types = []
  accessibility_desc = []
  block_ids = []
  properties =[]

  sqlFile.write("CREATE TABLE city_block(block_id int NOT NULL, clue_small_area varchar(30) NOT NULL, PRIMARY KEY(block_id));\n")
  sqlFile.write("CREATE TABLE accessibility_type(accessibility_rating int NOT NULL, accessibility_type varchar(35) NOT NULL, PRIMARY KEY(accessibility_rating));\n")
  sqlFile.write("CREATE TABLE accessibility_description(accessibility_id int NOT NULL, accessibility_rating int NOT NULL REFERENCES accessibility_type(accessibility_rating), accessibility_type_description varchar(85) NOT NULL, PRIMARY KEY(accessibility_id));\n")
  sqlFile.write("CREATE TABLE property(property_id int NOT NULL, base_property_id int NOT NULL, block_id int NOT NULL REFERENCES city_block(block_id), x_coordinate double, y_coordinate double, PRIMARY KEY(property_id, base_property_id));\n")
  sqlFile.write("CREATE TABLE census_building_data(census_year int NOT NULL, property_id int NOT NULL, base_property_id int NOT NULL, building_name varchar(65), street_address varchar(35), construction_year int, refurbished_year int, number_of_floors int, predominant_space_usage varchar(40), accessibility_id int REFERENCES accessibility_description(accessibility_id), bicycle_spaces int, has_showers boolean, FOREIGN KEY(property_id, base_property_id) REFERENCES property(property_id, base_property_id), PRIMARY KEY(census_year, property_id, base_property_id));\n")

  next(csvReader)
  
  for row in csvReader:
    if row[13] != "" and row[11] != "":
      if row[13] + ";" + row[11] not in accessibility_types:
        accessibility_types.append(str(row[13]) + ";" + str(row[11]))
        sqlFile.write("INSERT INTO accessibility_type (accessibility_rating, accessibility_type) VALUES (" + str(row[13]) + ", '" + str(row[11]) + "');\n")

    if row[13] != "" and row[12] != "":
      if row[13] + ";" + row[12] not in accessibility_desc:
        accessibility_desc.append(str(row[13]) + ";" + str(row[12]))
        new_id = str(len(accessibility_desc) - 1)
        sqlFile.write("insert into accessibility_description (accessibility_id, accessibility_rating, accessibility_type_description) values (" + new_id + ", " + str(row[13]) + ", '" + str(row[12]) + "');\n")

    if row[1] + ";" + row[6] not in block_ids:
      block_ids.append(str(row[1]) + ";" + str(row[6]))
      sqlFile.write("insert into city_block (block_id, clue_small_area) values (" + str(row[1]) + ", '" + str(row[6]) + "');\n")

    if row[2] + ";" + row[3] not in properties:
      if row[16] == "": 
        row[16] = "null"
      if row[17] == "": 
        row[17] = "null"
      properties.append(str(row[2]) + ";" + str(row[3]))
      sqlFile.write("insert into property (property_id, base_property_id, block_id, x_coordinate, y_coordinate) values (" + str(row[2]) + ", " + str(row[3]) + ", " + str(row[1]) + ", " + str(row[16]) + ", " + str(row[17]) + ");\n")

    if row[4] == "":
      row[4] = "null"
    else:
      row[4] = "'" + row[4].replace("'", "''") + "'"
    if row[7] == "":
      row[7] = "null"
    if row[8] == "":
      row[8] = "null"
    if row[11] == "":
      access_id = "null"
    else:
      access_id = str(accessibility_desc.index(str(row[13] + ";" + row[12])))
    if row[14] == "":
      row[14] = "null"
    if row[15] == "":
      row[15] = "null"
    elif row[15] == "0":
      row[15] = "'false'"
    elif row[15] == "1":
      row[15] = "'true'"
    sqlFile.write("insert into census_building_data (census_year, property_id, base_property_id, building_name, street_address, construction_year, refurbished_year, number_of_floors, predominant_space_usage, accessibility_id, bicycle_spaces, has_showers)" +
                      " values (" + str(row[0]) + ", " + str(row[2]) + ", " + str(row[3]) + ", " + str(row[4]) + ", '" + str(row[5].replace("'", "''")) + "', " + str(row[7]) + ", " + str(row[8]) + ", " + str(row[9]) + ", '" + str(row[10]) + "', " + access_id + ", " + str(row[14]) + ", " + str(row[15]) + ");\n")

csvFile.close()
print('Finished adding building records to census database.')
