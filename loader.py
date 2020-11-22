import pandas as pd
from typing import Dict, List, Optional, Union, Iterable
import json
from functools import partial
import os
import urllib.request as request
import shutil
import time

###############################################################################
#   User guide:
#   Call get_file_list().keys() to get a list of valid army names
#   Call download_armies(army_list) where army_list is a list of valid army names or a single valid army name. 
#       This will download the army data from Infinity army to infinity_inspector_files/
#       You do not need to specify the metadata file. That will always be downloaded.
#   Call load_all_downloaded_files() to get a dict of dataframes for the files you have downloaded
#   Example:
#   loader.download_armies(['yu_jing', 'haqqislam'])
#   armies = loader.load_all_downloaded_files()
#   haq_df = armies['haqqislam']
#
#   You can use regular pandas masking for attribute columns, and the helper methods .has_skill(skill_name), 
#   .has_weapon(weapon_name), and .has_equipment(equipment_name) to filter profiles. Use & and | to make logical
#   combinations of your masks. E.g:
#         haq_df[haq_df.has_skill('Minelayer') & ((haq_df['arm'] > 1) | haq_df.has_skill('Mimetism'))]
#   will show the haqqislam profiles that have the Minelayer skill and either more than 1 armour or the Mimetism skill.
#   
###############################################################################


################################################################################################
#    Helper subclass of DataFrame that makes masking for skills, weapons, and equipment easier
#################################################################################################

class DataFramePlus(pd.DataFrame):
    def __init__(self, *args):
        super(DataFramePlus, self).__init__(*args)
    
    def has(self, column, value):
        return self.apply(lambda x: value in x[column], axis=1)
    
    def has_weapon(self, value):
        return self.has('weapons', value)
    
    def has_skill(self, value):
        return self.has('skills', value)
    
    def has_equipment(self, value):
        return self.has('equipment', value)

#################################################################################################
#                                            File download
#################################################################################################
def get_file_list() -> Dict:
    return {
        'metadata':'https://api.corvusbelli.com/army/infinity/en/metadata',
        'yu_jing': 'https://api.corvusbelli.com/army/units/en/201',
        'imperial_service':'https://api.corvusbelli.com/army/units/en/202',
        'invincible_army': 'https://api.corvusbelli.com/army/units/en/204',
        'white_banner':'https://api.corvusbelli.com/army/units/en/205',
        'pan_oceania':'https://api.corvusbelli.com/army/units/en/101',
        'shock_army':'https://api.corvusbelli.com/army/units/en/102',
        'military_orders':'https://api.corvusbelli.com/army/units/en/103',
        'nca':'https://api.corvusbelli.com/army/units/en/104',
        'varuna':'https://api.corvusbelli.com/army/units/en/105',
        'winter_force':'https://api.corvusbelli.com/army/units/en/106',
        'ariadna':'https://api.corvusbelli.com/army/units/en/301',
        'cha':'https://api.corvusbelli.com/army/units/en/302',
        'merovingienne':'https://api.corvusbelli.com/army/units/en/303',
        'us_ariadna':'https://api.corvusbelli.com/army/units/en/304',
        'tac':'https://api.corvusbelli.com/army/units/en/305',
        'kosmoflot':'https://api.corvusbelli.com/army/units/en/306',
        'haqqislam':'https://api.corvusbelli.com/army/units/en/401',
        'hassassin':'https://api.corvusbelli.com/army/units/en/402',
        'qapu_khalqi':'https://api.corvusbelli.com/army/units/en/403',
        'ramah':'https://api.corvusbelli.com/army/units/en/404',
        'nomads':'https://api.corvusbelli.com/army/units/en/501',
        'corregidor':'https://api.corvusbelli.com/army/units/en/502',
        'bakunin':'https://api.corvusbelli.com/army/units/en/503',
        'tunguska':'https://api.corvusbelli.com/army/units/en/504',
        'combined_army':'https://api.corvusbelli.com/army/units/en/601',
        'morat':'https://api.corvusbelli.com/army/units/en/602',
        'shasvastii':'https://api.corvusbelli.com/army/units/en/603',
        'onyx':'https://api.corvusbelli.com/army/units/en/604',
        'aleph':'https://api.corvusbelli.com/army/units/en/701',
        'steel_phalanx':'https://api.corvusbelli.com/army/units/en/702',
        'oss':'https://api.corvusbelli.com/army/units/en/703',
        'tohaa':'https://api.corvusbelli.com/army/units/en/801',
        'druze':'https://api.corvusbelli.com/army/units/en/902',
        'jsa':'https://api.corvusbelli.com/army/units/en/903',
        'ikari_company':'https://api.corvusbelli.com/army/units/en/904',
        'starco':'https://api.corvusbelli.com/army/units/en/905',
        'spiral_corps':'https://api.corvusbelli.com/army/units/en/906',
        'foreign_company':'https://api.corvusbelli.com/army/units/en/907',
        'dashat_company':'https://api.corvusbelli.com/army/units/en/908',
        'white_company':'https://api.corvusbelli.com/army/units/en/909',
        'o-12':'https://api.corvusbelli.com/army/units/en/1001',
        'starmada':'https://api.corvusbelli.com/army/units/en/1002',
    }
    

def download_file(file_key: str, file_list: Dict, folder: str):
    if file_key not in file_list.keys():
        return False
    file_name = os.path.join(folder, file_key + '.json')
    if os.path.exists(file_name):
        os.remove(file_name)
    url = file_list[file_key]
    with request.urlopen(url) as response, open(file_name, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)
    return True

def verify_file_folder():
    folder = 'infinity_inspector_files'
    if not os.path.exists(folder):
        os.mkdir(folder)
    return folder

def download_armies(armies: Union[str, List[str]]):
    if type(armies) is not list:
        armies = [armies]
    
    folder = verify_file_folder()
    file_list = get_file_list()
    sleep_time = 0.3 #Add a short wait period between file requests to not overload server.
    
    print("Downloading metadata")
    result = download_file('metadata', file_list, folder)
    if not result:
        print('Failed to download metadata')
        return
    
    time.sleep(sleep_time)
    for army in armies:
        print("Downloading " + army)
        result = download_file(army, file_list, folder)
        if not result:
            print('Failed to download ' + army + '\nValid army names are:\n' + file_list.keys())
            return
        time.sleep(sleep_time)
    
#####################################################################################################
#                                            File Handling
#####################################################################################################
def load_all_downloaded_files():
    folder = verify_file_folder()
    files = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
    loaded_jsons = {}
    for file in files:
        with open(os.path.join(folder, file), encoding='utf-8') as f:
            var_name = os.path.basename(file).split('.')[0]
            loaded_jsons[var_name] = json.load(f)
    if 'metadata' not in loaded_jsons.keys():
        print("To load data, you must have the metadata file")
        return    
    metadata_df = load_metadata_to_dataframes(loaded_jsons['metadata'])
    loaded_jsons.pop('metadata')
    armies = {}
    for army_key in loaded_jsons.keys():
        print('Loading army: ' + army_key)
        armies[army_key] = load_army_data_to_dataframes(
            loaded_jsons[army_key], 
            metadata_df['weapons'],
            metadata_df['skills'],
            metadata_df['equipment'])
    
    return armies     
    
            
    
######################################################################################################
#                                            METADATA
######################################################################################################
def _load_skills_metadata_to_dataframe(skills_metadata: List) -> DataFramePlus:
    return DataFramePlus(skills_metadata)
    
def _load_weapons_metadata_to_dataframe(weapons_metadata: List, ammunition_df: DataFramePlus) -> DataFramePlus:
    weapons_df = DataFramePlus(weapons_metadata)
    # Need to convert damage and burst columns to a numeric format so that we can do queries later.
    weapons_df['damage_numeric'] = pd.to_numeric(weapons_df['damage'], errors='coerce')
    weapons_df['burst_numeric'] = pd.to_numeric(weapons_df['burst'], errors='coerce')
    # To make this easier to read, we want to cross-reference the ammunition values with the ammunition data to 
    # get readable names rather than id values
    ammunition_df_renamed = ammunition_df.rename(columns={'name':'ammunition', 'id': 'ammunition_id'}, inplace=False)[['ammunition_id', 'ammunition']]
    ammunition_df_renamed.columns
    weapons_df.rename(columns={'ammunition':'ammunition_id'}, inplace=True)
    weapons_df = pd.merge(weapons_df, ammunition_df_renamed, how='left')
    return weapons_df

def _load_equipment_metadata_to_dataframe(equipment_metadata: List) -> DataFramePlus:
    return DataFramePlus(equipment_metadata)

def _load_ammunition_metadata_to_dataframe(ammunition_metadata: List) -> DataFramePlus:
    return DataFramePlus(ammunition_metadata)

def _load_equipment_metadata_to_dataframe(equipment_metadata: List) -> DataFramePlus:
    return DataFramePlus(equipment_metadata)

def load_metadata_to_dataframes(metadata: Dict) -> Dict:
    dataframes = {}
    dataframes['ammunition'] = _load_ammunition_metadata_to_dataframe(metadata['ammunitions']) 
    dataframes['skills'] = _load_skills_metadata_to_dataframe(metadata['skills'])
    dataframes['weapons'] = _load_weapons_metadata_to_dataframe(metadata['weapons'], dataframes['ammunition'])
    dataframes['equipment'] = _load_equipment_metadata_to_dataframe(metadata['equips'])
    return dataframes
    
######################################################################################################
#                                         UNIT PROFILES
######################################################################################################
def add_column_from_dict_value(
        source_df: DataFramePlus, 
        column: str, 
        key: str, 
        new_column: Optional[str] = None) -> DataFramePlus:
    if(new_column is None):
        new_column = key
    source_df[new_column] = [d.get(key) for d in source_df[column]]
    return source_df

def id_to_name(id_list, lookup, extras_lookup):
    full_names = []
    for id in id_list:
        if id: # sometimes row is empty
            name = list(set(lookup[lookup['id'] == id['id']]['name'].tolist()))
            if 'extra' in id and id['extra']:
                extras = extras_lookup[extras_lookup['id'].isin(id['extra'])]['name'].tolist()
                extras = ['(' + extra + ')' for extra in extras]
            else:
                extras = []
            full_names.append(name[0] + ''.join(extras))
    return full_names    


def convert_skill_ids(army_df: DataFramePlus, skills_df: pd.DataFrame, extras_df: DataFramePlus) -> DataFramePlus:
    army_df.rename(columns={'skills':'skills_id'}, inplace=True)
    army_df.rename(columns={'option_skills':'option_skills_id'}, inplace=True)    
    
    id_to_skill = partial(id_to_name, lookup=skills_df, extras_lookup=extras_df)
    army_df['skills'] = army_df.apply(lambda row: id_to_skill(row['skills_id']), axis=1) + \
        army_df.apply(lambda row: id_to_skill(row['option_skills_id']), axis=1)

    return army_df

def set_attribute_column_types(army_df: DataFramePlus) -> DataFramePlus:
    return army_df

def convert_weapon_ids(army_df: DataFramePlus, weapons_df: DataFramePlus, extras_df: DataFramePlus) -> DataFramePlus:
    army_df.rename(columns={'weapons':'weapons_id'}, inplace=True)
    army_df.rename(columns={'option_weapons':'option_weapons_id'}, inplace=True)    
    
    id_to_weapon = partial(id_to_name, lookup=weapons_df, extras_lookup=extras_df)
    army_df['weapons'] = army_df.apply(lambda row: id_to_weapon(row['weapons_id']), axis=1) + \
        army_df.apply(lambda row: id_to_weapon(row['option_weapons_id']), axis=1)
    return army_df

def convert_equipment_ids(army_df: DataFramePlus, equipment_df: DataFramePlus, extras_df: DataFramePlus) -> DataFramePlus:
    army_df.rename(columns={'equip':'equip_id'}, inplace=True)
    army_df.rename(columns={'option_equipment':'option_equipment_id'}, inplace=True)    
    
    id_to_equipment = partial(id_to_name, lookup=equipment_df, extras_lookup=extras_df)
    army_df['equipment'] = army_df.apply(lambda row: id_to_equipment(row['equip_id']), axis=1) + \
        army_df.apply(lambda row: id_to_equipment(row['option_equipment_id']), axis=1)
    return army_df

def convert_type_ids(army_df: DataFramePlus) -> DataFramePlus:
    army_df.rename(columns={'type':'type_id'}, inplace=True)
    type_id_to_name = {
        1: 'LI',
        2: 'MI',
        3: 'HI',
        4: 'TAG',
        5: 'REM',
        6: 'SK',
        7: 'WB',
    }
    army_df['type'] = army_df.apply(lambda row: type_id_to_name[row['type_id']], axis=1)
    return army_df

def convert_columns_to_type(df: DataFramePlus, columns: List[str], type: str) -> DataFramePlus:
    for col in columns:
        df[col] = df[col].astype(type)
    return df

def get_orders_of_type(orders_list: List[Dict], order_type: str) -> int:
    count = sum([order['total'] for order in orders_list if order['type']==order_type])
    #print(order_type + ' orders in ' + str(orders_list) + ' totalled to: ' + str(count))
    return count

def add_order_columns(df: DataFramePlus) -> DataFramePlus:    
    #df[['Regular Orders', 'Irregular Orders', 'Impetious Orders', 'Lieutenant Orders']] = df.apply( 
    #    lambda row: [get_orders_of_type(row['orders'], order_type) for order_type in ['REGULAR', 'IRREGULAR', 'IMPETUOUS', 'LIEUTENANT']], axis=1)
    order_cols = [df.apply(lambda row: get_orders_of_type(row['orders'], order_type), axis=1) for order_type in ['REGULAR', 'IRREGULAR', 'IMPETUOUS', 'LIEUTENANT']]
    df['Regular Orders'] = order_cols[0]
    df['Irregular Orders'] = order_cols[1]
    df['Impetuous Orders'] = order_cols[2]
    df['Lieutenant Orders'] = order_cols[3]
    return df
    
def load_army_data_to_dataframes(army_data: Dict, weapons_df: DataFramePlus, skills_df: DataFramePlus, equipment_df: DataFramePlus) -> DataFramePlus:
    pd.options.display.max_colwidth = 200
    pd.set_option('display.max_columns', None)
    army_df = DataFramePlus(army_data['units'])
    extras_df = DataFramePlus(army_data['filters']['extras'])
    
    # Expand each profile group to its own row. 
    # Profile groups are for units that have multiple "units" such as:
    #    - Maghariba Guard (TAG and Pilot), 
    #    - Scarface and Cordelia (Tag, Pilot, Engineer)
    # Note that for a profile like Carmen Johns and Bâtard, there are two entries in 
    # profileGroups, one for Carmen and one for Bâtard. The mounted/dismounted profiles for 
    # Carmen are handled as separate entries in profileGroup['profiles']
    army_df = army_df.explode('profileGroups', ignore_index=True)
    
    army_df = add_column_from_dict_value(source_df=army_df, column='profileGroups', key='options', new_column='profileGroupOptions')
    army_df = add_column_from_dict_value(source_df=army_df, column='profileGroups', key='profiles', new_column='profileGroupProfiles')
    
    # Handle options
    army_df = army_df.explode('profileGroupOptions', ignore_index=True)
    # Put points and swc in their own columns
    army_df = add_column_from_dict_value(source_df=army_df, column='profileGroupOptions', key='points')
    army_df = add_column_from_dict_value(source_df=army_df, column='profileGroupOptions', key='swc')
    # Extract extra skills that belong to the profile. These will be added to the main skill list later
    army_df = add_column_from_dict_value(source_df=army_df, column='profileGroupOptions', key='skills', new_column='option_skills')
    army_df = add_column_from_dict_value(source_df=army_df, column='profileGroupOptions', key='orders') # Todo: Reformat into something more easy to parse
    army_df = add_column_from_dict_value(source_df=army_df, column='profileGroupOptions', key='peripheral', new_column='option_peripheral')
    army_df = add_column_from_dict_value(source_df=army_df, column='profileGroupOptions', key='weapons', new_column='option_weapons')
    army_df = add_column_from_dict_value(source_df=army_df, column='profileGroupOptions', key='equip', new_column='option_equipment')     
    army_df = army_df.explode('profileGroupProfiles', ignore_index=True)
    army_df = add_column_from_dict_value(source_df=army_df, column='profileGroupProfiles', key='skills')
    for key in ['skills', 'arm', 'ava', 'bs', 'bts', 'cc', 'move', 'ph', 's', 'str', 'w', 'wip', 'equip', 'weapons', 'peripheral', 'type', 'chars']:
        army_df = add_column_from_dict_value(source_df=army_df, column='profileGroupProfiles', key=key)
    
    army_df = convert_skill_ids(army_df, skills_df, extras_df)
    army_df = convert_weapon_ids(army_df, weapons_df, extras_df)
    army_df = convert_equipment_ids(army_df, equipment_df, extras_df)
    army_df = set_attribute_column_types(army_df)
    army_df = convert_type_ids(army_df)
    army_df = add_order_columns(army_df)
    
    
    army_df = army_df.drop(['id', 'idArmy', 'canonical', 'isc', 'iscAbbr', 'profileGroups', 'options', 'slug', 'filters', 'notes', 'profileGroupOptions', 'profileGroupProfiles', 'option_skills_id', 'option_weapons_id', 'skills_id', 'weapons_id', 'equip_id', 'option_equipment_id', 'option_peripheral', 'peripheral'], axis=1)    
    army_df[army_df["name"].isna()] = ''
    army_df = army_df[army_df['name']!='']
    army_df = convert_columns_to_type(army_df, ['arm', 'w', 'ava', 'bs', 'bts', 'cc', 'ph', 'points', 'wip'], 'int32')    
    army_df.loc[army_df['swc'] == '-', 'swc'] = '0'
    army_df = convert_columns_to_type(army_df, ['swc'], 'float')
    army_df = army_df[[
        'name', 
        'type', 
        'move', 
        'cc', 'bs', 'ph', 'wip', 'arm', 'bts', 'w', 'str', 's', 'ava', 
        'skills', 
        'equipment', 
        'weapons',
        'swc', 'points', 
        'Regular Orders', 'Irregular Orders', 'Impetuous Orders', 'Lieutenant Orders']]


    
    return DataFramePlus(army_df)




# Notes:
# category = 1 -> Garrison troops
# category = 2 -> Line troops
# category = 3 -> Specially Trained Troops
# category = 4 -> Veteran Troops
# category = 5 -> Elite troops
# category = 6 -> Headquarters troops
# category = 7 -> Mechanized troops
# category = 8 -> Support troops
# 
# category = 10 -> Character
# category = 11 -> Mercenary

# type = 1 -> Light Infantry
# type = 2 -> Medium Infantry
# type = 3 -> Heavy Infantry
# type = 4 -> TAG
# type = 5 -> REM
# type = 6 -> Skirmishers
# type = 7 -> Warbands
