import pandas as pd
from bs4 import BeautifulSoup
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
MAX_WORKERS = 5
def parse_html(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
    except Exception as e:
        print(f"Ошибка {url}: {e}")
        return None
    soup = BeautifulSoup(res.text, "lxml")
    table = soup.find("table", class_="results")
    if not table:
        return None

    tbody = table.find("tbody")
    if not tbody:
        return None

    rows = tbody.find_all("tr")
    if len(rows) != 1:
        return None

    row = rows[0]
    cells = row.find_all("td")
    if len(cells) < 4:
        return None

    name_tag = cells[0].find("a")
    name = name_tag.get_text(strip=True) if name_tag else cells[0].get_text(strip=True)
    imo = cells[1].get_text(strip=True)
    mmsi = cells[2].get_text(strip=True)
    ship_type = cells[3].get_text(strip=True)

    return {
        "Название": name,
        "IMO": imo,
        "MMSI": mmsi,
        "Тип": ship_type,
        "Ссылка": url
    }

def main():
    file = "Links.xlsx"

    data_frame = pd.read_excel(file, usecols=[0], header=0)
    list_links = data_frame['Ссылка'].dropna().tolist()

    # for i in list_links:
    #     print(i + "\n")
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(parse_html, url): url for url in list_links}
        for future in as_completed(future_to_url):
            data = future.result()
            if data:
                results.append(data)
                print(f"Обработано: {data['Название']}")
    if results:
        result_df = pd.DataFrame(results)
        result_df.to_excel("result.xlsx", index=False)
    else:
        print("\nНи одно судно не найдено.")



if __name__ == "__main__":
    main()

