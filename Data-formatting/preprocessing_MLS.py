import pandas as pd
import numpy as np
import os


# preprocessing options
binaryCardOccurency = True
# TODO: add Objectives
# Objectives can be: ....
featureSetObjective = []

tracked_team = "LAFC"

matches = os.listdir("../Data/MLS_raw/")

unique_players = []
opp_unique_players = []
away_game = False
data = pd.DataFrame(columns=["own_lineup", "score", "opposing_lineup", "opposing_team"])


def abbreviate(name):
    if "--" in name:
        players = name.split("--")
        return abbreviate(players[0]) + "--" + abbreviate(players[1])
    names = name.split(" ")
    if len(names) == 3:
        if names[1] == "de" or names[1] == "van":
            names = [names[0], names[1] + " " + names[2]]
    return names[0][0] + ". " + names[-1]


def abbreviate_team(team):
    mask = team["line"] == "bench"
    team.loc[mask, "name"] = team["name"].apply(lambda n: abbreviate(n))
    return team


for match_day in matches:
    m = pd.read_pickle("../Data/MLS_raw/" + match_day + "/match_data")
    t1 = pd.read_pickle("../Data/MLS_raw/" + match_day + "/team1")
    t2 = pd.read_pickle("../Data/MLS_raw/" + match_day + "/team2")

    if match_day.split("-")[-1] == tracked_team:
        own_team = t2
        opp_team = t1
    else:
        own_team = t1
        opp_team = t2

    # Inconsistancy between LAFC lineups and timeline names
    own_team = own_team.replace(
        [
            "Bragança",
            "D. Jaković",
            "N. Hamalainen",
            "J. Pérez",
            "M. Munir",
            "Ilie",
            "K. Moon-Hwan",
            "J. Méndez",
        ],
        [
            "J. Moutinho",
            "D. Jakovic",
            "N. Hämäläinen",
            "J. Perez",
            "M. Monir",
            "I. Sánchez",
            "K. Moon-hwan",
            "J. Mendez",
        ],
    )
    # opp_team = opp_team.replace("J. Alfaro", "T. Alfaro")

    # abbreviate the names in the data
    # own_team= abbreviate_team(own_team)
    # opp_team = abbreviate_team(opp_team)
    own_team["name"] = own_team["name"].apply(lambda n: abbreviate(n))
    opp_team["name"] = opp_team["name"].apply(lambda n: abbreviate(n))
    m["name"] = m["name"].apply(lambda n: abbreviate(n))

    own_team = own_team.replace(
        [
            "Bragança",
            "D. Jaković",
            "N. Hamalainen",
            "J. Pérez",
            "M. Munir",
            "Ilie",
            "K. Moon-Hwan",
            "J. Méndez",
        ],
        [
            "J. Moutinho",
            "D. Jakovic",
            "N. Hämäläinen",
            "J. Perez",
            "M. Monir",
            "I. Sánchez",
            "K. Moon-hwan",
            "J. Mendez",
        ],
    )

    game_players = list(own_team["name"])
    new_players = list(set(game_players).difference(unique_players))
    unique_players.extend(new_players)
    for player in new_players:
        data["own_" + player] = 0

    opp_game_players = list(opp_team["name"])
    opp_new_players = list(set(opp_game_players).difference(opp_unique_players))
    opp_unique_players.extend(opp_new_players)
    for player in opp_new_players:
        data["opp_" + player] = 0

    own_starting_11 = list(own_team[own_team["line"] != "bench"]["name"])
    opp_starting_11 = list(opp_team[opp_team["line"] != "bench"]["name"])

    own_lineup = "-".join(
        map(str, list(own_team.line.value_counts()[own_team.line.unique()])[1:-1])
    )
    opp_lineup = "-".join(
        map(str, list(opp_team.line.value_counts()[opp_team.line.unique()])[1:-1])
    )

    # reverse the match data
    m_r = m.loc[::-1]
    score = [0, 0]
    for i, e in m_r.iterrows():
        temp_data = pd.DataFrame(
            {
                "own_lineup": [own_lineup],
                "opposing_lineup": opp_lineup,
                "opposing_team": opp_team["team"].loc[0],
            }
        )
        if e["record"] == "goal":
            if e["team_pos"].split(" ")[0] == tracked_team:
                score[0] += 1
                temp_data["score"] = "-".join(list(map(str, score)))
                for player in own_starting_11:
                    temp_data["own_" + player] = 1
                for player in opp_starting_11:
                    temp_data["opp_" + player] = -1
            else:
                score[1] += 1
                temp_data["score"] = "-".join(list(map(str, score)))
                for player in own_starting_11:
                    temp_data["own_" + player] = -1
                for player in opp_starting_11:
                    temp_data["opp_" + player] = 1
            data = pd.concat([data, temp_data], ignore_index=True)
        if e["record"] == "own_goal":
            if e["team_pos"].split(" ")[0] == tracked_team:
                score[1] += 1
                temp_data["score"] = "-".join(list(map(str, score)))
                for player in own_starting_11:
                    temp_data["own_" + player] = -1
                for player in opp_starting_11:
                    temp_data["opp_" + player] = 1
            else:
                score[0] += 1
                temp_data["score"] = "-".join(list(map(str, score)))
                for player in own_starting_11:
                    temp_data["own_" + player] = 1
                for player in opp_starting_11:
                    temp_data["opp_" + player] = -1
            data = pd.concat([data, temp_data], ignore_index=True)
        if e["record"] == "substitution":
            p_in, p_out = e["name"].split("--")
            if e["team_pos"].split(" ")[0] == tracked_team:
                own_starting_11.remove(p_out)
                own_starting_11.append(p_in)
            else:
                opp_starting_11.remove(p_out)
                opp_starting_11.append(p_in)
        if e["record"] == "red_card":
            p_out = e["name"]
            if e["team_pos"].split(" ")[0] == tracked_team:
                own_starting_11.remove(p_out)
            else:
                opp_starting_11.remove(p_out)
        if e["record"] == "yellow_card":
            # TODO: track yellow card data
            continue

data = data.fillna(0)

data.to_csv(path_or_buf="../Data/MLS-" + tracked_team + "_own_stats.csv", header=True)
