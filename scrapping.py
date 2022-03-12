import os  # helper functions like check file exists
import datetime  # automatic file name
import requests  # the following imports are common web scraping bundle
from urllib.request import urlopen  # standard python module
from bs4 import BeautifulSoup
from urllib.error import HTTPError
from collections import defaultdict
import re
from urllib.error import URLError
from tqdm import tqdm
import pickle
import bz2
import _pickle as cPickle
import pandas as pd

import nltk
nltk.download('stopwords')
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk import bigrams
def extract_theme(link):
    try:
        theme_text = re.findall(r'.fr/.*?/', link)[0]
    except:
        pass
    else:
        return theme_text[4:-1]


def list_themes(links):
    themes = []
    for link in links:
        theme = extract_theme(link)
        if theme is not None:
            themes.append(theme)
    return themes


def write_links(path, links, year_fn):
    with open(os.path.join(path + "/lemonde_" + str(year_fn) + "_links.txt"), 'w') as f:
        for link in links:
            f.write(link + "\n")


def write_to_file(filename, content):
    if os.path.exists(filename):
        with open(filename, 'a+') as f:
            f.write(str(content))
    else:
        with open(filename, 'w') as f:
            f.write(str(content))

def create_archive_links(year_start, year_end, month_start, month_end, day_start, day_end):
    archive_links = {}
    for y in range(year_start, year_end + 1):
        dates = [str(d).zfill(2) + "-" + str(m).zfill(2) + "-" +
                 str(y) for m in range(month_start, month_end + 1) for d in
                 range(day_start, day_end + 1)]
        archive_links[y] = [
            "https://www.lemonde.fr/archives-du-monde/" + date + "/" for date in dates]
    return archive_links

def get_articles_links(archive_links):
    links_non_abonne = []
    for link in archive_links:
        try:
            html = urlopen(link)
        except HTTPError as e:
            print("url not valid", link)
        else:
            soup = BeautifulSoup(html, "html.parser")
            news = soup.find_all(class_="teaser")
            # condition here : if no span icon__premium (abonnes)
            for item in news:
                if not item.find('span', {'class': 'icon__premium'}):
                    l_article = item.find('a')['href']
                    # en-direct = video
                    if 'en-direct' not in l_article:
                        links_non_abonne.append(l_article)
    return links_non_abonne


def classify_links(theme_list, link_list):
    dict_links = defaultdict(list)
    for theme in theme_list:
        theme_link = 'https://www.lemonde.fr/' + theme + '/article/'
        for link in link_list:
            if theme_link in link:
                dict_links[theme].append(link)
    return dict_links


def get_single_page(url):
    try:
        html = urlopen(url)
    except HTTPError as e:
        print("url not valid", url)
    else:
        soup = BeautifulSoup(html, "html.parser")
        text_title = soup.find('h1')
        text_body = soup.article.find_all(["p", "h2"], recursive=False)
        return (text_title, text_body)

def scrape_articles(dict_links):
    themes = dict_links.keys()
    tokenizer = RegexpTokenizer(r'\w+')
    stopWords = set(stopwords.words('french'))
    df = {}
    df['bigram'] = []
    df['date'] = []
    df['article_title'] = []
    df['theme'] = []
    for theme in themes:
        counter = 0
        for i in tqdm(range(len(dict_links[theme]))):
            link = dict_links[theme][i]
            year = link.split('/')[5]
            month = link.split('/')[6]
            day = link.split('/')[7]
            date = year + '/' + month + '/' + day
            single_page = get_single_page(link)
            if single_page is not None:
                title = single_page[0].get_text()
                for line in single_page[1]:
                    sentence = line.get_text()
                    words = tokenizer.tokenize(sentence)
                    wordsFiltered = []
                    for w in words:
                        if w.lower() not in stopWords:
                            if not w.isdigit():
                                wordsFiltered.append(w.lower())
                    modified_sentence_body = ' '.join([w for w in wordsFiltered])
                    wordsFiltered_bigrams_tmp=bigrams(modified_sentence_body.split())
                    wordsFiltered_bigrams_sentence = []
                    for a in wordsFiltered_bigrams_tmp:
                        wordsFiltered_bigrams_sentence.append(a[0]+" "+a[1])
                    for c in wordsFiltered_bigrams_sentence :
                        counter += 1
                        if counter > 2000:
                            break
                        else :
                            df['bigram'].append(c)
                            df['date'].append(date)
                            df['article_title'].append(title)
                            df['theme'].append(theme)

    df = pd.DataFrame(df)
    df.to_csv('bigrams.csv')





def create_folder(path):
    if not os.path.exists(path):
        os.mkdir(path)
    else:
        print("folder exists already")

archive_links = create_archive_links(2022,2022,1, 2, 1, 31)
corpus_path = os.path.join(os.getcwd(), "corpus_links")
create_folder(corpus_path)
article_links = {}
for year,links in archive_links.items():
    print("processing: ",year)
    article_links_list = get_articles_links(links)

    article_links[year] = article_links_list
    write_links(corpus_path,article_links_list,year)
themes = []
for link_list in article_links.values():
    themes.extend(list_themes(link_list))
from collections import Counter
theme_stat = Counter(themes)
theme_top = []
for k,v in sorted(theme_stat.items(), key = lambda x:x[1], reverse=True):
    if v > 120:
        theme_top.append((k, v))
print(theme_top)
all_links = []
for link_list in article_links.values():
    all_links.extend(link_list)
themes_top_five = [x[0] for x in theme_top[:5]]
themes_top_five_links = classify_links(themes_top_five,all_links)
scrape_articles(themes_top_five_links)
