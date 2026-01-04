from concurrent.futures import ProcessPoolExecutor as Pool


import pandas as pd
import random
import glob
import numpy as np


def median_and_std(file: str):
    df = pd.read_csv(file, sep=";", encoding="utf-8-sig")
    group = (df.groupby("Category")["Value"].agg(
        Median="median", Std="std").reset_index())
    return group


def calc_stats(item):
    letter, numbers = item
    return letter, np.median(numbers), np.std(numbers, ddof=0)


def main():
    count = 1000
    result1 = []
    result2 = []
    obj = {"A": [],
           "B": [],
           "C": [],
           "D": []}
    for i in range(5):
        table = {"Category": [chr(random.randint(65, 68)) for i in range(
            count)], "Value": [random.uniform(1, 1000000) for i in range(count)]}
        df = pd.DataFrame(table)
        df.to_csv(f'table{i}.csv', sep=";", encoding="utf-8-sig", index=False)
    file = glob.glob("table*.csv")
    with Pool() as pool:
        for res in pool.map(median_and_std, file):
            result1.append(res)

    new_df = pd.concat(result1, ignore_index=True)
    print("Часть 1 \n")
    for _, row in new_df.iterrows():

        print(row["Category"], row["Median"], row["Std"])

    print("\n Часть 2")

    for i in range(len(new_df)):
        result2.append([new_df.loc[i, "Category"], new_df.loc[i, "Median"]])

    for cat, med in result2:
        if cat in obj:
            obj[cat].append(med)

    result2.clear()
    with Pool() as pool:
        for char, median, std_median in pool.map(
                calc_stats, [(k, v) for k, v in obj.items()]):
            result2.append((char, median, std_median))

    for char, median, std_median in result2:
        print(
            f"{char}: медиана медиан = {median}, отклонение медиан = {std_median}")


if __name__ == "__main__":
    main()