import pandas as pd
import re

df_a = pd.read_csv('data/a_lvr_land_a.csv')
df_b = pd.read_csv('data/b_lvr_land_a.csv')
df_e = pd.read_csv('data/e_lvr_land_a.csv')
df_f = pd.read_csv('data/f_lvr_land_a.csv')
df_h = pd.read_csv('data/h_lvr_land_a.csv')

# there are an unnecessary english title row
df_all = pd.concat([df_a[1:], df_b[1:], df_e[1:], df_f[1:], df_h[1:]])

###
# for filter_a
###


# can not filter floors directly
def floor_to_int(floor_string):
    num_mapping = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
    result = 0
    now = 1
    try:
        for c in floor_string:
            if c == '層':
                return result + now
            elif c == '十':
                result += now * 10
                now = 0
            elif c == '百' or c == '佰':
                result += now * 100
                now = 0
            elif c == '零':
                continue
            else:
                try:
                    now = num_mapping[c]
                except KeyError as e:
                    return 0
        return result + now
    except Exception as e:
        # logging.warning('can not handle floor: ' + str(floor_string) + '; cast as 0')
        return 0


# df_with_floor = pd.concat([df_all, df_all['總樓層數'].map(floor_to_int).rename('floor_int')], axis=1)
int_floor = df_all['總樓層數'].map(floor_to_int)
constraint = (df_all['主要用途'] == '住家用') & (df_all['建物型態'] == '住宅大樓(11層含以上有電梯)') & (int_floor >= 13)

filtered_df = df_all[constraint]
filtered_df.to_csv('filter_a.csv', index=False)


###
# for filter_b
###
all_number = df_all['鄉鎮市區'].count()


def get_type_numbers(type_string):
    # temp = re.findall(r'\d+|\D+', type_string)
    temp = re.split(r'(\d+)', type_string)
    result = {}
    for i in range(0, len(temp)-1, 2):
        result[temp[i]] = int(temp[i+1])
    return pd.Series(result)


def simple_car(type_string):
    return int(type_string.split('車位')[-1])


# types = df_all['交易筆棟數'].apply(get_type_numbers)
parking_lot = df_all['交易筆棟數'].apply(simple_car)
parking_lot_number = parking_lot.sum()

all_price_mean = df_all['總價元'].astype(int).mean()
parking_lot_price = df_all['車位總價元'].astype(int).sum()

pd.DataFrame.from_dict({
    '總件數': [all_number],
    '總車位數': [parking_lot_number],
    '平均總價元': [all_price_mean],
    '平均車位總價元': [parking_lot_price / parking_lot_number]
}).to_csv('filter_b.csv', index=False)

