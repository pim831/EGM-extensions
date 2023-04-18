import pandas as pd
import numpy as np
from itertools import chain
import json

# preprocessing options
binaryCardOccurency = True
# Objectives can be: power, thoughness, types or manaCost
featureSetObjective = []


print("loading data...")
df = pd.read_csv("../Data/KHM_draft_raw.csv", ',')


print("Start preprocessing")
# Fetch data from 17Lands data source
cards = []
for i in df.columns:
    if i.startswith("deck_"):
        cards.append(i)

# Select relevant data
relevant_attributes = ["user_win_rate_bucket", "won", *cards]
relevant_data = df[relevant_attributes]
# relevant_data.columns = relevant_data.columns.str.removeprefix("deck_")

# Fetch card properties from corresponding mtgJson set
with open("../Data/mtgjson/KHM.json", encoding="utf-8") as f:
    card_data = json.load(f)

f.close()

card_props = {}
for j, i in enumerate(card_data["data"]["cards"]):
    data = {}
    data["colors"] = i.get("colors")
    data["manaCost"] = i.get("manaCost")
    data["power"] = i.get("power")
    data["toughness"] = i.get("toughness")
    data["types"] = i.get("types")
    name = i["name"].split(sep=" // ")[0]
    card_props[name] = data


unique_features = {}
for feature in featureSetObjective:
    allAppearingFeatures = [v[feature] for k, v in card_props.items()]
    allAppearingFeatures = [i for i in allAppearingFeatures if i is not None]

    if (feature == "types"):
        unique = {tuple(my_list) for my_list in allAppearingFeatures}
        result = [list(tup) for tup in unique]
        result.append(None)
        unique_features[feature] = result
    else:
        result = list({i for i in allAppearingFeatures})
        result.append(None)
        unique_features[feature] = result


def calculate_deck_colors(cards):
    cards_in_deck = cards[cards >= 1].index
    cards_in_deck = list(map(lambda x: x[5:], cards_in_deck))
    colors = list(map(lambda x: card_props[x]["colors"], cards_in_deck))
    roles = set(chain(*colors))
    roles = ''.join(roles)
    return roles


# To avoid pandas from getting confused when inserting sets
relevant_data.insert(2, "roles", np.empty((len(relevant_data), 0)).tolist())

for index, row in relevant_data.iterrows():
    colors = calculate_deck_colors(row[3:])
    relevant_data.at[index, 'roles'] = colors

for index, row in relevant_data.iterrows():
    for objective in featureSetObjective:
        df_obj = pd.DataFrame(columns=unique_features[objective])
        dict = {key: 0 for key in unique_features[objective]}
        cards = row[3:]
        cid = cards[cards >= 1].index
        for ci in cid:
            ob = card_props[ci[5:]][objective]
            dict[ob] += cards[cid[0]]
        df_dict = pd.DataFrame(dict, index=[index])
        df_obj = pd.concat([df_obj, df_dict])
        df_obj.add_prefix(objective + '_')


if binaryCardOccurency:
    for card in relevant_data.columns[3:]:
        relevant_data.loc[relevant_data[card] >= 1, card] = 1

relevant_data.columns = relevant_data.columns.str.replace(" ", "")
relevant_data.columns = relevant_data.columns.str.replace(",", "")
relevant_data.columns = relevant_data.columns.str.replace("'", "")
relevant_data.columns = relevant_data.columns.str.replace("-", "")


# Possibly more efficient way of getting all the relevant features
# def featureSelect(feature, df):
#     i = 0
#     for column in df.columns[3:]:
#         df[column] = df[column].map(lambda x: 0 if x == 0 else card_props[column[5:]][feature])
#     return df

relevant_data.to_csv(
    path_or_buf="../Data/KHM_draft_clean_binary.csv", header=True)
