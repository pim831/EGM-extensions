import pandas as pd
import numpy as np
from itertools import chain
import json

# preprocessing options
binaryCardOccurency = True
# Objectives can be: power, thoughness, types or manaCost
featureSetObjective = []


# provided by 17lands.com https://www.17lands.com/public_datasets
"""Replay data dtypes for pandas"""

MAX_MULLIGANS = 7
MAX_TURNS = 30

BASE_COLS = {
    "build_index": "int8",
    "draft_id": str,
    "draft_time": str,
    "event_type": str,
    "expansion": str,
    "format": str,
    "game_index": "int8",
    "game_number": "int8",
    "history_id": int,
    "main_colors": str,
    "match_number": "int8",
    "missing_diffs": "int8",
    "num_mulligans": "int8",
    "num_turns": "int8",
    "on_play": bool,
    "opp_colors": str,
    "opp_num_mulligans": "int8",
    "opp_rank": str,
    "oppo_deck_colors": str,
    "oppo_mulligans": "int8",
    "oppo_rank": str,
    "rank": str,
    "splash_colors": str,
    "time": str,
    "turns": "int8",
    "user_deck_colors": str,
    "user_game_win_rate_bucket": "float16",
    "user_mulligans": "int8",
    "user_n_games_bucket": "int8",
    "user_rank": str,
    "won": bool,
}

PER_TURN_COLS = {
    "cards_discarded": str,
    "cards_drawn": str,
    "cards_foretold": str,
    "creatures_attacked": str,
    "creatures_blitzed": "int8",
    "creatures_blocked": str,
    "creatures_blocking": str,
    "creatures_cast": str,
    "creatures_unblocked": str,
    "eot_oppo_cards_in_hand": str,
    "eot_oppo_creatures_in_play": str,
    "eot_oppo_lands_in_play": str,
    "eot_oppo_life": "int8",
    "eot_oppo_non_creatures_in_play": str,
    "eot_user_cards_in_hand": str,
    "eot_user_creatures_in_play": str,
    "eot_user_lands_in_play": str,
    "eot_user_life": "int8",
    "eot_user_non_creatures_in_play": str,
    "lands_played": str,
    "non_creatures_cast": str,
    "oppo_abilities": str,
    "oppo_cards_learned": str,
    "oppo_combat_damage_taken": "int16",
    "oppo_creatures_blitzed": "int8",
    "oppo_creatures_killed_combat": str,
    "oppo_creatures_killed_non_combat": str,
    "oppo_instants_sorceries_cast": str,
    "oppo_mana_spent": "int8",
    "player_combat_damage_dealt": "int8",
    "user_abilities": str,
    "user_cards_learned": str,
    "user_combat_damage_taken": "int16",
    "user_creatures_blitzed": "int8",
    "user_creatures_killed_combat": str,
    "user_creatures_killed_non_combat": str,
    "user_instants_sorceries_cast": str,
    "user_mana_spent": "int8",
}

SUMMARY_COLS = {
    "cards_discarded": "int8",
    "cards_drawn": "int8",
    "cards_foretold": "int8",
    "cards_learned": "int8",
    "creatures_blitzed": "int8",
    "creatures_cast": "int8",
    "instants_sorceries_cast": "int8",
    "lands_played": "int8",
    "mana_spent": "int16",
    "non_creatures_cast": "int8",
}


def get_dtypes():
    dtypes = BASE_COLS.copy()

    for x in range(1, MAX_MULLIGANS + 1):
        dtypes[f"candidate_hand_{x}"] = str
    dtypes["opening_hand"] = str

    for turn in range(1, MAX_TURNS + 1):
        for player in ["user", "oppo"]:
            for k, v in PER_TURN_COLS.items():
                dtypes[f"{player}_turn_{turn}_{k}"] = v
            dtypes[f"{player}_turn_{turn}_eot_oppo_cards_in_hand"] = "int8"
        dtypes[f"oppo_turn_{turn}_cards_drawn"] = "int8"

    for player in ["user", "oppo"]:
        for col in SUMMARY_COLS:
            dtypes[f"{player}_total_{col}"] = "int8"

    return dtypes


# eta for STX 4 minutes
print("loading data...")
replay = pd.read_csv("../Data/STX_draft_replay_raw.csv", ",")
draft = pd.read_csv("../Data/STX_draft_raw.csv", ",")


print("Start preprocessing...")


def intersection(lst1, lst2):
    temp = set(lst2)
    lst3 = [value for value in lst1 if value in temp]
    return lst3


# eta for STX 1 minute
print("Filter out incomplete data")
# i.e. unknown draft ids so the replay data can't be coupled to the draft data
l1 = list(replay["draft_id"].unique())
l2 = list(draft["draft_id"].unique())

complete_ids = intersection(l1, l2)

replay = replay[replay["draft_id"].isin(complete_ids)]
draft = draft[draft["draft_id"].isin(complete_ids)]

print("Sort data on draft id...")

replay.sort_values(by=["draft_id"], inplace=True)
draft.sort_values(by=["draft_id"], inplace=True)


replay.drop_duplicates(subset="draft_id", keep="first", inplace=True)
relevant_replay_info = replay[["draft_id", "user_deck_colors", "oppo_deck_colors"]]

print("Join data on match...")
join = pd.merge(relevant_replay_info, draft, on="draft_id", how="outer")
join.drop(columns=['draft_id'], inplace=True)

print("Format the data...")
cards = []
for i in join.columns:
    if i.startswith("deck_"):
        cards.append(i)

# Select relevant data
relevant_attributes = ["user_deck_colors", "oppo_deck_colors","user_win_rate_bucket", "won", *cards]
relevant_data = join[relevant_attributes]


relevant_data.rename(columns={"user_deck_colors": "roles"}, inplace=True)

setroles = []
for i in relevant_data["roles"].unique():
    s = set(i)
    ap = False
    if len(setroles) > 0:
        for j, r in enumerate(setroles):
            if s == set(r[0]):
                setroles[j].append(i)
                ap = True
                break
    if ap:
        continue
    setroles.append([i])

for l in setroles:
    if len(l) > 0:
        relevant_data.loc[relevant_data["roles"].isin(l), "roles"] = l[0]

# Fetch card properties from corresponding mtgJson set
with open("../Data/mtgjson/STX.json", encoding="utf-8") as f:
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

    if feature == "types":
        unique = {tuple(my_list) for my_list in allAppearingFeatures}
        result = [list(tup) for tup in unique]
        result.append(None)
        unique_features[feature] = result
    else:
        result = list({i for i in allAppearingFeatures})
        result.append(None)
        unique_features[feature] = result


if binaryCardOccurency:
    for card in relevant_data.columns[5:]:
        relevant_data.loc[relevant_data[card] >= 1, card] = 1

relevant_data.columns = relevant_data.columns.str.replace(" ", "")
relevant_data.columns = relevant_data.columns.str.replace(",", "")
relevant_data.columns = relevant_data.columns.str.replace("'", "")
relevant_data.columns = relevant_data.columns.str.replace("-", "")


relevant_data.to_csv(path_or_buf="../Data/STX_replay_clean_binary.csv", header=True)
