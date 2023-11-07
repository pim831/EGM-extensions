import heapq
import pandas as pd
import numpy as np

print("loading data...")
# df = pd.read_csv("KHMtraddraft_binary.csv",';')
df = pd.read_csv("../../Data/KHM_draft_clean.csv", ',', index_col=0)

# Remove spaces from card names
# df.columns = df.columns.str.replace(" ", "")
# df.columns = df.columns.str.replace(",", "")
df.columns = df.columns.str.replace("-", "")

support_floor = 200

card_thresholds = [4,2,1]

blacklist = ['deck_Plains', 'deck_Island',
             'deck_Swamp', 'deck_Mountain', 'deck_Forest']

def threshold_uniformity(value, thresholds = card_thresholds):
    for t in thresholds:
        if value >= t:
            return t
    return 0

# ETA 5-10 minutes
# Dynamic thresholds
for c in df.columns[3:]:
    if c in blacklist:
        continue
    vc = df[c].value_counts().sort_index()
    thresholds = []
    floor = 0
    for i, v in reversed(list(enumerate(vc.index))):
        floor += vc.values[i]
        if floor >= support_floor:
            thresholds.append(v)
            floor = 0
    if len(thresholds) <= 1:
        # add features that don't appear to the blacklist as they aren't worth looking at
        blacklist.append(c)
        continue
    df_temp = pd.DataFrame(df[c].apply(threshold_uniformity, thresholds = thresholds).astype(str))
    new_columns = df_temp.join(df_temp[c].str.get_dummies()).drop(columns=[c]).add_prefix(c + '_')
    df = df.drop(columns = [c])
    df = df.join(new_columns)



# Make a list of features that we will make subgroup descriptions from
# card columns have names that start with deck_
prefix = "deck_"
prefix_len = len(prefix)
# we don't want conditions on the normal basics
blacklist = ['deck_Plains', 'deck_Island',
             'deck_Swamp', 'deck_Mountain', 'deck_Forest']
# fetch the card names; could be useful to have
cardnames = [col[prefix_len:]
             for col in df.columns if col.startswith(prefix) and col not in blacklist]
# make the list of features
features = ['deck_'+cardname for cardname in cardnames]

# Use instead if not using a prefix
# features = list(df.columns[3:])

# for quick testing: restrict to first 10 features
# features = features[:10]


####################################################################################################################################
# EMM framework


#

class BoundedPriorityQueue:
    """
    Ensures uniqness
    Keeps a maximum size (throws away value with least quality)
    """

    def __init__(self, bound):
        self.values = []
        self.bound = bound
        self.entry_count = 0

    def add(self, element, quality, **adds):
        # if any((e == element for (_, _, e) in self.values)):
        #     return  # avoid duplicates
        # if any((self.desc_intersect(element, e) for (_,_,e) in self.values)):
        #    return
        new_entry = (quality, self.entry_count, element, adds)
        if (len(self.values) >= self.bound):
            heapq.heappushpop(self.values, new_entry)
        else:
            heapq.heappush(self.values, new_entry)

        self.entry_count += 1

    def get_values(self):
        for (q, _, e, x) in sorted(self.values, reverse=True):
            yield (q, e, x)

    def show_contents(self):  # for debugging
        print("show_contents")
        for (q, entry_count, e) in self.values:
            print(q, entry_count, e)

#


class Queue:
    """
    Ensures uniqness
    """

    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        if item not in self.items:
            self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

    def get_values(self):
        return self.items

    def add_all(self, iterable):
        for item in iterable:
            self.enqueue(item)

    def clear(self):
        self.items.clear()

#


def EMM(w, d, q, eta, satisfies_all, eval_quality, catch_all_description):
    """
    w - width of beam
    d - num levels
    q - max results
    eta - a function that receives a description and returns all possible refinements
    satisfies_all - a function that receives a description and verifies wheather it satisfies some requirements as needed
    eval_quality - returns a quality for a given description. This should be comparable to qualities of other descriptions
    catch_all_description - the equivalent of True, or all, as that the whole dataset shall match
    """
    resultSet = BoundedPriorityQueue(q)
    candidateQueue = Queue()
    candidateQueue.enqueue(catch_all_description)
    for level in range(d):
        print("level : ", level)
        beam = BoundedPriorityQueue(w)
        for seed in candidateQueue.get_values():
            print("    seed : ", seed)
            for desc in eta(seed):
                if satisfies_all(desc):
                    quality = eval_quality(desc)
                    resultSet.add(desc, quality)
                    beam.add(desc, quality)
        candidateQueue = Queue()
        candidateQueue.add_all(desc for (_, desc, _) in beam.get_values())
    return resultSet

####################################################################################################################################


def refine(desc, more):
    copy = desc[:]
    copy.append(more)
    return copy


def eta(seed):
    print("eta ", seed)
    if seed != []:  # we only specify more on the elements that are still in the subset
        d_str = as_string(seed)
        ind = df.eval(d_str)
        df_sub = df.loc[ind, ]
    else:
        df_sub = df
    for f in features:
        if f[:5] == "deck_":
            # we only want positive boolean constraints on cards for our subgroups
            candidate = f
            if not candidate in seed:  # if not already there
                yield refine(seed, candidate)

        # For the Magic EGM paper we do not build descriptions with features that are not cards.
        # The cases below are left over from the general EMM code and might be useful if you add
        # more/different subgroup descriptions.

        # get quantiles here instead of intervals for the case that data are very skewed
        elif (df_sub[f].dtype == 'float64'):
            column_data = df_sub[f]
            dat = np.sort(column_data)
            dat = dat[np.logical_not(np.isnan(dat))]
            for i in range(1, 6):  # determine the number of chunks you want to divide your data in
                x = np.percentile(dat, 100/i)
                candidate = "{} <= {}".format(f, x)
                if not candidate in seed:  # if not already there
                    yield refine(seed, candidate)
                candidate = "{} > {}".format(f, x)
                if not candidate in seed:  # if not already there
                    yield refine(seed, candidate)
        elif (df_sub[f].dtype == 'object'):
            column_data = df_sub[f]
            uniq = column_data.dropna().unique()
            for i in uniq:
                candidate = "{} == '{}'".format(f, i)
                if not candidate in seed:  # if not already there
                    yield refine(seed, candidate)
                candidate = "{} != '{}'".format(f, i)
                if not candidate in seed:  # if not already there
                    yield refine(seed, candidate)
        elif (df_sub[f].dtype == 'int64'):
            column_data = df_sub[f]
            dat = np.sort(column_data)
            dat = dat[np.logical_not(np.isnan(dat))]
            for i in range(1, 6):  # determine the number of chunks you want to divide your data in
                x = np.percentile(dat, 100/i)
                candidate = "{} <= {}".format(f, x)
                if not candidate in seed:  # if not already there
                    yield refine(seed, candidate)
                candidate = "{} > {}".format(f, x)
                if not candidate in seed:  # if not already there
                    yield refine(seed, candidate)
        elif (df_sub[f].dtype == 'bool'):
            uniq = column_data.dropna().unique()
            for i in uniq:
                candidate = "{} == '{}'".format(f, i)
                if not candidate in seed:  # if not already there
                    yield refine(seed, candidate)
                candidate = "{} != '{}'".format(f, i)
                if not candidate in seed:  # if not already there
                    yield refine(seed, candidate)
        else:
            assert False


def as_string(desc):
    return ' and '.join(desc)


def satisfies_all(desc):
    # only consider subgroups with sufficient support
    d_str = as_string(desc)
    ind = df.eval(d_str)
    return sum(ind) >= support_floor


# names of the columns indicating the roles
basic_lands = {
    'W': "deck_Plains",
    'U': "deck_Island",
    'B': "deck_Swamp",
    'R': "deck_Mountain",
    'G': "deck_Forest",
}

# make a string description for a role set


def roleset_desc_as_string(roleset):
    role = []
    for color in "WUBRG":
        if color in roleset:
            role.append(basic_lands[color]+'==True')
        else:
            role.append(basic_lands[color]+'==False')
    return ' and '.join(role)

# Compute the conditional winrate for a subgroup description and a roleset.
# Returns -1 if this combination has insufficient support.


def conditional_winrate(desc, roleset):
    d_str = ' and '.join([as_string(desc), 'roles=="'+ roleset + '"'])
    subset = df[df.eval(d_str)]
    if subset.shape[0] < support_floor:
        return -1
    else:
        winrate = subset['won'].mean()
        return winrate

# Enumerate nonempty strict strings of s


def nonempty_strict_subsets(s, added_something=False, dropped_something=False):
    if s == "":
        if added_something and dropped_something:
            yield ""
    else:
        for rest in nonempty_strict_subsets(s[1:], added_something, True):
            yield rest
        for rest in nonempty_strict_subsets(s[1:], True, dropped_something):
            yield s[0] + rest

# The EGM quality measure
def eval_quality(desc):
    # get role set set
    rss = df[df.eval(as_string(desc)).astype(bool)]['roles'].unique()

    # keep track of the maximizer
    best_quality = 0
    best_roleset = "-"
    best_winrate = 0
    best_rolesubset = "-"
    best_subwinrate = 0

    # cache winrate for each role set with sufficient support; store in dictionary
    winrate = {}
    for roleset in rss:
        wr = conditional_winrate(desc, roleset)
        if wr != -1:
            winrate[roleset] = wr

    # calculate quality for all the rolesets
    for roleset in rss:
        if len(roleset) > 4:
            continue
        if roleset not in winrate:
            continue  # skip if it doesn't have support
        winrate_self = winrate[roleset]
        for subset in nonempty_strict_subsets(roleset):
            if subset in winrate:  # only look at it if it has support
                winrate_sub = winrate[subset]
                quality_here = winrate_self - winrate_sub
                if quality_here > best_quality:
                    best_quality = quality_here
                    best_roleset = roleset
                    best_winrate = winrate_self
                    best_rolesubset = subset
                    best_subwinrate = winrate_sub
    return best_quality, best_roleset, best_winrate, best_rolesubset, best_subwinrate


# actually run the experiment
EGM_res = EMM(100, 3, 100, eta, satisfies_all, eval_quality,
              [])  # second parameter is d (the depth)

print()
print()
print('===================')
print()
print()

headers = ["Quality", "Description"]
exc_results = []
for (q, d, adds) in EGM_res.get_values():
    exc_results.append([q, d])
    print(q, d, adds)

# save to CSV
pd.DataFrame(exc_results, columns=headers).to_csv(
    "output.csv", index=False, sep=";")
