import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_values
import time
import threading

def main():
    has_more_page = True
    limit = 7500

    connection_params = {
        "host": "localhost",
        "database": "animes",
        "user": "postgres",
        "password": "Master@123"
    }

    insert_query = """
                        INSERT INTO animes (title, link, image, genres, episodes, studios, status) 
                        VALUES %s
                        ON CONFLICT (title) DO UPDATE
                        SET link = EXCLUDED.link,
                            image = EXCLUDED.image,
                            genres = EXCLUDED.genres,
                            episodes = EXCLUDED.episodes,
                            studios = EXCLUDED.studios,
                            status = EXCLUDED.status;
                        """
    start_time = time.time()
    try:
        conn = psycopg2.connect(**connection_params)

        cursor = conn.cursor()

        while has_more_page:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Cookie': 'MALHLOGSESSID=1c7e8d5d7e89b2f4d1c5c11dc369191b; usprivacy=1NNY; m_gdpr_mdl_20230227=1; MALSESSIONID=2f9ot8fgp80j2c95v03fgbvtu3; is_logged_in=1; aws-waf-token=502bd38a-5d7f-4e66-8898-e52d5b513580:CQoAiOpGM5gJAAAA:AjpNjJoiRXoS3JYet1bNBskgXANK5hhX3sj4NL77hWvM2sQ6rjojrSv8IekkxGw4AXbIevhq9idu6FRR2RyDod8EFdqrHhtRqcRBNgDSLzVE0m+1UzIYr5FnTxrfpX3CxYuwbq4ZCHstQ8GrNBrOvIRkb7xhY17k/+RF6iJITF2KuoCJTA4dSdVrUMkoadrRJHDMRcBk5A4Nlsxek/TWl1c4ik/bEscCw4NW22TYIpGxr9lMDo2gXt95ExsIcpviV3vXmPNh8ARC; mal_cache_key=5cec156bdc1d2ab1c9c2fd945532d30b'
            }
            req = requests.get(f'https://myanimelist.net/topanime.php?type=tv&limit={limit}', headers=headers)
            soup = BeautifulSoup(req.content, "html.parser")
            animes = []

            threads = []

            elements = soup.find_all("tr")

            if elements:
                for element in elements:
                    title = element.find("h3")
                    if title:
                        thread = threading.Thread(target=parse_element, args=(title, headers, animes))
                        thread.start()
                        threads.append(thread)
                
                for thread in threads:
                    thread.join()
            else:
                has_more_page = False

            print(animes)
            limit += 50
            print(limit)

            values = [(anime["title"], anime["link"], anime["image"], anime["genres"], anime["episodes"], anime["studios"], anime["status"]) for anime in animes]

            execute_values(cursor, insert_query, values)

            conn.commit()
    except (Exception, Error) as error:
        print("Error: ", error)               

    cursor.close()
    conn.close()

    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("Elapsed time:", elapsed_time, "seconds")
    

def parse_element(title, headers, animes):
    anime = {
        "title": None,
        "link": None,
        "image": None,
        "genres": [],
        "episodes": None,
        "studios": [],
        "status": None,
    }

    anchor = title.find("a")
    anime["title"] = anchor.text

    anchor_href = anchor.get("href")
    anime["link"] = anchor_href

    sub_req = requests.get(anchor_href, headers=headers)
    sub_soup = BeautifulSoup(sub_req.content, "html.parser")

    sub_element = sub_soup.find("div", class_="leftside")
    
    img = sub_element.find("img")
    if img:
        img_src = img.get("data-src")
        anime["image"] = img_src

    if anime["image"] == None:
        anime["image"] = f'https://placehold.co/600x400?text={anime["title"]}'

    spaceit_pad_divs = sub_element.find_all("div", class_="spaceit_pad")

    for spaceit_pad in spaceit_pad_divs:
        spaceit_pad_text = spaceit_pad.get_text(strip=True)
        if "Episodes" in spaceit_pad_text:
            episode_number = spaceit_pad_text.split(":")[1].strip()
            anime["episodes"] = episode_number
        elif "Studios" in spaceit_pad_text:
            studio = spaceit_pad_text.split(":")[1].strip()
            studios = []

            if 'None found' in studio:
                studios.extend([])
            elif "," in studio:
                studio_split = spaceit_pad_text.split(",")
                studios.extend(studio_split)
            else:
                studios.append(studio)

            anime["studios"] = studios
        elif "Status" in spaceit_pad_text:
            status = spaceit_pad_text.split(":")[1].strip()
            anime["status"] = status

        genre_spans = spaceit_pad.find_all("span", itemprop="genre")
        genres = [span.get_text() for span in genre_spans]
        if len(genres) > 0:
            anime["genres"] = genres
            break
    animes.append(anime)

main()