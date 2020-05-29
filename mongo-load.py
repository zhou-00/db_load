import csv, pymongo
from pymongo import MongoClient


def delete_empty(d: dict) -> dict:
    newDict = {k:v if type(v) is not dict else delete_empty(v) for k,v in d.items() if v != ''}
    return {k:v for k,v in newDict.items() if v != {}}


def main():
    client = MongoClient('mongodb://127.0.0.1:27017/')

    db=client.census

    csvPath = 'Buildings_with_name__age__size__accessibility__and_bicycle_facilities.csv'

    with open(csvPath, encoding='utf-8-sig') as csvFile:
        
        csvReader = csv.DictReader(csvFile)
        
        added_count = 0
        
        for row in csvReader:
            
            building = {'census_year' : row['Census year'],
                        'building_id' : { 'block_id' : row['Block ID'],
                                  'property_id' : row['Property ID'],
                                  'base_property_id' : row['Base property ID'] },
                        'building_name' : row['Building name'],
                        'address' : {'street_address' : row['Street address'],
                                     'suburb' : row['CLUE small area'] },
                        'history' : { 'construction_year' : row['Construction year'],
                                      'refurbished_year' : row['Refurbished year']},
                        'floors_above_ground' : row['Number of floors (above ground)'],
                        'space_use' : row['Predominant space use'],
                        'accessibility' : { 'type' : row['Accessibility type'],
                                            'type_description' : row['Accessibility type description'],
                                            'rating' : row['Accessibility rating'] },
                        'amenities' : { 'bicycle_spaces' : row['Bicycle spaces'],
                                        'has_showers' : row['Has showers'] },
                        'location' : { 'x_coord' : row['x coordinate'],
                                       'y_coord' : row['y coordinate'],
                                       'coord_point' : row['Location'] }
            }

            building = delete_empty(building)
            result = db.buildings.insert_one(building)
            added_count += 1
            print('Created {} record as {}'.format(added_count, result.inserted_id))

    csvFile.close()
    print('Finished adding building records to census database.')


if __name__ == "__main__":
    main()

# fieldnames = ['census_year', '_id', 'building_name', 'street_address', 'clue_area', 'building_history', 'floors', 'space_use', 'accessibility', 'amenities', 'location']
# nested fields: id, building_history, accessibility, amenities, location

