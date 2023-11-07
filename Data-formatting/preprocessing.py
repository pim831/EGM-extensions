import pandas as pd
import numpy as np
from itertools import chain
from tqdm import tqdm
import json

# preprocessing options
binaryCardOccurency = True
# Objectives can be: power, thoughness, types or manaCost
featureSetObjective = ['types', 'power', 'toughness']

# color degrees
color_degrees = False


print("loading data...")
df = pd.read_csv("../Data/KHM_draft_raw.csv", sep=",")


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
    data["colorIdentity"] = i.get("colorIdentity")
    data["convertedManaCost"] = i.get("convertedManaCost")
    data["keywords"] = i.get("keywords")
    data["rarity"] = i.get("rarity")
    name = i["name"].split(sep=" // ")[0]
    card_props[name] = data


unique_features = {}
for feature in featureSetObjective:
    allAppearingFeatures = [v[feature] for k, v in card_props.items()]
    allAppearingFeatures = [i for i in allAppearingFeatures if i is not None]

    if feature == "types" or feature == "colorIdentity" or feature == "colors":
        unique = {tuple(my_list) for my_list in allAppearingFeatures}
        result = [list(tup) for tup in unique]
        result.append(None)
        unique_features[feature] = result
    elif feature == "keywords":
        result = list(set(chain.from_iterable(allAppearingFeatures)))
        result.append('None')
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
    roles = "".join(roles)
    return roles


def calculate_deck_color_percentages(cards):
    W = 0
    U = 0
    B = 0
    R = 0
    G = 0
    C = 0

    W_lands = 0
    U_lands = 0
    B_lands = 0
    R_lands = 0
    G_lands = 0

    for i, c in enumerate(cards.index):
        count = cards.values[i]
        # only look at cards who actually appear in the deck
        if count == 0:
            continue
        mCost = card_props[c[5:]]["manaCost"]
        # Land
        if mCost == None:
            ci = card_props[c[5:]]["colorIdentity"]
            for c in ci:
                if c == "W":
                    W_lands += count
                elif c == "U":
                    U_lands += count
                elif c == "B":
                    B_lands += count
                elif c == "R":
                    R_lands += count
                elif c == "G":
                    G_lands += count
            continue
        pips = mCost.split("}{")
        if len(pips) == 1:
            pips = [pips[0][1]]
        else:
            pips = [pips[0][1:], *pips[1:-1], pips[-1][:-1]]
        for p in pips:
            if p == "W":
                W += count
            elif p == "U":
                U += count
            elif p == "B":
                B += count
            elif p == "R":
                R += count
            elif p == "G":
                G += count
            elif p == "X":
                continue
            else:
                C += int(p) * count
    color_presence_abs = np.array([W, U, B, R, G])
    color_presence_rel = color_presence_abs / color_presence_abs.sum()
    land_presence_abs = np.array([W_lands, U_lands, B_lands, R_lands, G_lands])
    land_presence_rel = land_presence_abs / land_presence_abs.sum()
    return color_presence_rel, land_presence_rel


print("Computing feature counts...")
columns = []
for feature in unique_features:
    t = type(unique_features[feature][0])
    for v in unique_features[feature]:
        if v == None:
            columns.append(feature + ':_None')
            continue
        if t == list:
            v = ''.join(v)
        columns.append(feature + ':_' + str(v))

feature_count = pd.DataFrame(columns=columns)
feature_list = []
for index, row in tqdm(relevant_data.iterrows()):
    feature_dict = dict.fromkeys(columns, 0)
    cards = row[2:]
    deck = cards[cards >=1]
    deck_indices = deck.index
    deck_props = list(map(lambda x: card_props[x[5:]],deck_indices))
    for i, c in zip(deck.values, deck_props):
        for feature in unique_features:
            volume = c[feature]
            if feature == 'keywords':
                if volume == None:
                    feature_dict[feature + ':_None'] += i
                    continue
                for v in volume:
                    if v == None:
                        feature_dict[feature + ':_None'] += i
                        continue
                    if type(v) == list:
                        v = ''.join(v)
                    feature_dict[feature + ':_' + str(v)] += i
            else:
                v = volume
                if v == None:
                    feature_dict[feature + ':_None'] += i
                    continue
                if type(v) == list:
                    v = ''.join(v)
                feature_dict[feature + ':_' + str(v)] += i
    pd_temp = pd.DataFrame(data=feature_dict, index = [index])
    if index % 500 == 499:
        feature_list.append(feature_count.copy())
        feature_count = pd.DataFrame(columns=columns)
    feature_count = pd.concat([feature_count, pd_temp], ignore_index=True)
feature_list.append(feature_count)

feature_count = pd.concat(feature_list, ignore_index=True)

# To avoid pandas from getting confused when inserting sets
relevant_data.insert(2, "roles", np.empty((len(relevant_data), 0)).tolist())

if color_degrees:
    # eta: 30 minutes for KHM
    colors = pd.DataFrame(
        columns=[
            "W",
            "U",
            "B",
            "R",
            "G",
            "W_lands",
            "U_lands",
            "B_lands",
            "R_lands",
            "G_lands",
        ]
    )
    for index, row in tqdm(relevant_data.iterrows(), total=relevant_data.shape[0]):
        mCost_colors, lands_colors = calculate_deck_color_percentages(row[3:])
        percentages = [*mCost_colors, *lands_colors]
        colors.loc[index] = percentages
    relevant_data = pd.concat([colors, relevant_data], axis=1)
else:
    for index, row in relevant_data.iterrows():
        colors = calculate_deck_colors(row[3:])
        relevant_data.at[index, "roles"] = colors


relevant_data = relevant_data.join(feature_count)

# for index, row in relevant_data.iterrows():
#     for objective in featureSetObjective:
#         df_obj = pd.DataFrame(columns=unique_features[objective])
#         dict = {key: 0 for key in unique_features[objective]}
#         cards = row[3:]
#         cid = cards[cards >= 1].index
#         for ci in cid:
#             ob = card_props[ci[5:]][objective]
#             dict[ob] += cards[cid[0]]
#         df_dict = pd.DataFrame(dict, index=[index])
#         df_obj = pd.concat([df_obj, df_dict])
#         df_obj.add_prefix(objective + "_")


if binaryCardOccurency:
    for card in relevant_data.columns[3:324]:
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

relevant_data.to_csv(path_or_buf="../Data/KHM_draft_clean_binary.csv", header=True)
