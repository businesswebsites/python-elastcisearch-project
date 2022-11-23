# -*- coding: utf8 -*-
from datetime import datetime
from email import message
from unittest import result
from urllib import response
from elasticsearch7 import Elasticsearch
import requests
from bs4 import BeautifulSoup as bs
import json
from PyPDF2 import PdfFileReader
import PyPDF2
import io
import base64
import tika
tika.TikaClientOnly = True
from tika import parser
import icd10
import simple_icd_10 as icd
import pandas as pd
from  es_synonyms import load_synonyms
from elasticsearch_dsl import (
    analyzer,
    char_filter,
    token_filter,
)
import csv
import codecs
import urllib
#pdfminer
from io import StringIO

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser

import glob
from pdfminer.high_level import extract_text
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
#nltk.download('stopwords')
from nltk.tokenize import RegexpTokenizer
german_stop_words = stopwords.words('german')

#webscraper für die dateien zum download der Langfassung
neue_liste = []
title_list = []
liste_von_links = [
"https://www.awmf.org/leitlinien/leitlinien-suche/ll-ergebnis/liste/ll-seite/0/ll-dok/lang/ll-klass/alle/ll-gesellschaft/0/ll-org/0/ll-sort/rel/ll-erg/100.html?tx_szleitlinien_pi2%5Bstatus%5D=1",
"https://www.awmf.org/leitlinien/leitlinien-suche/ll-ergebnis/liste/ll-seite/100/ll-dok/lang/ll-klass/alle/ll-gesellschaft/0/ll-org/0/ll-sort/rel/ll-erg/100.html?tx_szleitlinien_pi2%5Bstatus%5D=1",
"https://www.awmf.org/leitlinien/leitlinien-suche/ll-ergebnis/liste/ll-seite/200/ll-dok/lang/ll-klass/alle/ll-gesellschaft/0/ll-org/0/ll-sort/rel/ll-erg/100.html?tx_szleitlinien_pi2%5Bstatus%5D=1",
"https://www.awmf.org/leitlinien/leitlinien-suche/ll-ergebnis/liste/ll-seite/300/ll-dok/lang/ll-klass/alle/ll-gesellschaft/0/ll-org/0/ll-sort/rel/ll-erg/100.html?tx_szleitlinien_pi2%5Bstatus%5D=1",
"https://www.awmf.org/leitlinien/leitlinien-suche/ll-ergebnis/liste/ll-seite/400/ll-dok/lang/ll-klass/alle/ll-gesellschaft/0/ll-org/0/ll-sort/rel/ll-erg/100.html?tx_szleitlinien_pi2%5Bstatus%5D=1",
"https://www.awmf.org/leitlinien/leitlinien-suche/ll-ergebnis/liste/ll-seite/500/ll-dok/lang/ll-klass/alle/ll-gesellschaft/0/ll-org/0/ll-sort/rel/ll-erg/100.html?tx_szleitlinien_pi2%5Bstatus%5D=1",
"https://www.awmf.org/leitlinien/leitlinien-suche/ll-ergebnis/liste/ll-seite/600/ll-dok/lang/ll-klass/alle/ll-gesellschaft/0/ll-org/0/ll-sort/rel/ll-erg/100.html?tx_szleitlinien_pi2%5Bstatus%5D=1",
"https://www.awmf.org/leitlinien/leitlinien-suche/ll-ergebnis/liste/ll-seite/700/ll-dok/lang/ll-klass/alle/ll-gesellschaft/0/ll-org/0/ll-sort/rel/ll-erg/100.html?tx_szleitlinien_pi2%5Bstatus%5D=1",
"https://www.awmf.org/leitlinien/leitlinien-suche/ll-ergebnis/liste/ll-seite/800/ll-dok/lang/ll-klass/alle/ll-gesellschaft/0/ll-org/0/ll-sort/rel/ll-erg/100.html?tx_szleitlinien_pi2%5Bstatus%5D=1"
]

#liste_von_links = ["https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-internistische-intensivmedizin-und-notfallmedizin-dgiin.html"]

"""liste_von_links = ["https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-allgemein-und-viszeralchirurgie.html",

"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-gefaesschirurgie.html",
"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-handchirurgie.html",
"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-hals-nasen-ohren-heilkunde-kopf-und-hals-chirurgie-e-v-bonn-dg-hno.html",
"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-kinderchirurgie-dgkic.html",
"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-mund-kiefer-und-gesichtschirurgie.html",
"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-neurochirurgie-dgnc.html",
"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-orthopaedie-und-orthopaedische-chirurgie-e-v.html",
"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-orthopaedie-und-unfallchirurgie-dgou.html",
"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-thorax-gefaess-und-herzchirurgie-dgthg.html",
"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-thoraxchirurgie-dgt.html",
"https://www.awmf.org/leitlinien/aktuelle-leitlinien/ll-liste/deutsche-gesellschaft-fuer-unfallchirurgie-ev.html"

]"""

for q in range(len(liste_von_links)):
    seite = requests.get(liste_von_links[q])
    soup = bs(seite.text, "html.parser")
    for data in soup.find_all("h3"):
        for a in data.find_all("a"):
            neue_liste.append("https://www.awmf.org/"+ a.get("href"))
            title_list.append(a.get("title"))

"""for q in range(len(liste_von_links)):
    seite = requests.get(liste_von_links[q])
    soup = bs(seite.text, "html.parser")
    for data in soup.find_all("div", {"class": "col-title"}):
        for a in data.find_all("a"):
            neue_liste.append("https://www.awmf.org/"+ a.get("href"))
            title_list.append(a.get("title"))
    for data in soup.find_all("div", {"class": "col-title col1"}):
        for a in data.find_all("a"):
            neue_liste.append("https://www.awmf.org/"+ a.get("href"))
            title_list.append(a.get("title"))"""

title_list = list(dict.fromkeys(title_list))
neue_liste = list(dict.fromkeys(neue_liste))
#print(title_list)
#print(neue_liste)

#title_list = []
#new_val = soup.find_all("h3")

#erstellt title-list
#for i in new_val:
#    for j in i.find_all("a"):
#        new_new_val = j.get("title")
        #print(new_new_val)
#    title_list.append(new_new_val)

#gibt title-list aus
#for i in range(0, len(title_list)):
#    print(title_list[i])
#    print(i)

#download the file
#for i in range(96, len(neue_liste)):
#    r = requests.get(neue_liste[i], allow_redirects=True)
#    open(str(title_list[i])+".pdf", 'wb').write(r.content)



"""with io.BytesIO(response.content) as f:
    pdf = PdfFileReader(f)
    number_of_pages = pdf.getNumPages()
    newText = ""
    for i in range(0, number_of_pages):
        page1 = pdf.getPage(i)
        text = page1.extractText().split()
        for j in text:
            newText = newText + j + " "
    print(newText)"""

#ab hier wichtig für die Indexierung
#glob listet alle PDF-Dateien
liste_pdfs = glob.glob("*.pdf")
#print(liste_pdfs)
final_list = []
#Abschnitt der gedownloaded werden soll
"""for i in range(380, len(liste_pdfs)):
    final_list.append(liste_pdfs[i])
    print(i)
    print(liste_pdfs[i])"""

#text = extract_text(liste_pdfs[0])
#print(text)

"""output_string = StringIO()
with io.BytesIO(response.content) as f:
    pdf = PDFParser(f)
    doc = PDFDocument(pdf)
    rsrcmgr = PDFResourceManager()
    device = TextConverter(rsrcmgr, output_string, laparams=LAParams())
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    this_doc = ''
    for page in PDFPage.create_pages(doc):
        interpreter.process_page(page)
        this_text = output_string.getvalue()
        this_doc += this_text
        #print(this_text)

new_list = []
for i in range(0, 9):
    new_list.append(neue_liste[i])"""

es = Elasticsearch()

#Funktion für die Filterung von Namen und Inhalt der PDF-Files
"""def extractPdfFiles(files):
    i = 0
    this_loc = 1
    df = pd.DataFrame(columns = ("name", "content"))
    for file in files:
            this_doc = ''    
            this_doc += extract_text(file)
            df.loc[this_loc] = file, this_doc
            this_loc = this_loc + 1
    return df

df = extractPdfFiles(final_list)
df.head()

#Indexierung der PDF-Files
col_names = df.columns
for row_number in range(df.shape[0]):
    body = dict([(name, str(df.iloc[row_number][name])) for name in col_names])
    #es.index(index='datas', doc_type='pdf_files', body=body)
    es.index(index="a1", doc_type="files_a1", body=body)"""


"""search_results = es.search(index='a1', doc_type='files_a1', 
            body={
              "from" : 0, "size" : 1000,
                "_source":"name",
                "query":{
                    "match_all":{}
                }
            }
)"""
#Liste der Suchbegriffe, die von der AWMF vorgegeben wurden
query_liste = ["postpartale Depression", 
"schwere chronisch obstruktive Lungenerkrankung",
"Störungen des Sozialverhaltens",
"Lymphödem",
"Dermatose",
"Periimplantitis",
"Pneumothorax",
"Gallensteine",
"Aortendissektion",
"Sexuell übertragbare Infektionen"
]
print("Ab hier wird die erste Suchanfrage in Elasticsearch übergeben")
synonyms = query_liste[0]
search_results = es.search(index='a1', doc_type='files_a1', 
            body={
                "from" : 0, "size" : 1000,
                "_source":"name",
                "query":{
                    "match_phrase":{
                        "content":synonyms
                        
                    }
                }
            }
)

#id_liste = []
#print(search_results["hits"]["hits"])
#for i in range(0, len(search_results["hits"]["hits"])):
value_x = len(search_results['hits']['hits'])
val_x = 0
if value_x < 10:
    val_x = value_x
else:
    val_x = 10

print("Hier kommt die Ergebnisliste der Suchanfrage für den ersten Suchbegriff:")
print("")
ergebnisliste = []
for i in range(0,val_x):
    print(search_results['hits']['hits'][i]['_source']['name'])
    p = search_results["hits"]["hits"][i]["_source"]["name"]
    ergebnisliste.append(p[:-4])
    print(search_results['hits']['hits'][i]['_score'])
    #print(search_results['hits']['hits'][i]['_id'])

#löschen der doppelten IDs
#for i in id_liste:
#    es.delete(index='a1', doc_type='files_a1', id=i)
#print(ergebnisliste)

#ab hier NLTK, also Titel ohne Füllwörter in einer Liste speichern
#Funktion für die Tokenisierung erstellen
def tokenizing(test_text):
    text_tokens = word_tokenize(test_text)
    tokenizer = RegexpTokenizer(r'\w+')
    new_text_tokens = tokenizer.tokenize(test_text)
    tokens_without_sw = [word for word in new_text_tokens if not word in stopwords.words()]
    return tokens_without_sw


tokens_without_sw = tokenizing(ergebnisliste[0])
print("Hier ist die tokenisierte Liste von Wörtern für das erste Ergebnis:")
print(tokens_without_sw)

#ab hier themenverwandte Leitlinien finden
#Variante 1 gesamter Titel der Leitlinien in ES werfen und Ergebnis beobachten
mega_liste = []
"""for j in range(0, len(ergebnisliste)):
    erg_var_1 = es.search(index='a1', doc_type='files_a1', 
                body={
                    "from" : 0, "size" : 10,
                    "_source":"name",
                    "query":{
                        "match_phrase":{
                            "content":ergebnisliste[j]
                            
                        }
                    }
                }
    )
    #print(len(erg_var_1))
    print(len(erg_var_1['hits']['hits']))
    #continue wenn liste leer ist
    if(len(erg_var_1['hits']['hits'])==0):
        continue
    else:
        #jede leitlinie in mega_liste speichern, hierbei -2 gewählt, weil sonst irgendwie index out of range
        for i in range(0,len(erg_var_1)-2):
            print(erg_var_1['hits']['hits'][i]['_source']['name'])
            mega_liste.append(erg_var_1['hits']['hits'][i]['_source']['name'])

#duplicate remove
mega_liste = list(dict.fromkeys(mega_liste))
print(mega_liste)"""

#hier themenverwandte Leitlinien in Variante 2
#gefilterte Wörter in ES werfen -> Ergebnis sind themenverwandte
#anschließend gefilterte Wörter noch einmal in Synonymlisten werfen und dann in ES

#hier zuerst ohne Synonymlisten
print("")
print("Hier werden die tokenisierten Wörter in Elasticsearch übergeben")
res_list = []
syns = tokens_without_sw
for i in range(0, len(syns)):
    res = es.search(index='a1', doc_type='files_a1', 
                body={
                    "from" : 0, "size" : 1000,
                    "_source":"name",
                    "query":{
                        "match_phrase":{
                            "content":syns[i]
                            
                        }
                    }
                }
    )
    """x_var = 0
    if(len(res)>10):
        x_var = 10
    else:
        x_var = len(res)"""
    x_var = len(res['hits']['hits'])
    for j in range(0, x_var):
        res_list.append(res['hits']['hits'][j]['_source']['name'])


#Stelle in der Liste von Links finden, um den Link anschließend zu öffnen und die Referenzliste zu erscrapen
#index_val = title_list.index(ergebnisliste[0])       #  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! HIER AUCH AUSKOMMENTIERT !!!!!!!!!!!!!!!!!!!!!!!!!!
print("Ab hier kommt die Referenzliste für den Vergleich hinzu")

#ab hier Referenzliste besorgen 
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  AUSKOMMENTIERT VON HIER !!!!!!!!!!!!!!!!!!!!!!!!!!
"""ref_list = []
ref_link_list = []
new_seite = requests.get(neue_liste[index_val])
new_soup = bs(new_seite.text, "html.parser")
counter_val = 0
for data in new_soup.find_all("ul", attrs={"class":"ref-list"}):
    for a in data.find_all("a"):
        ref_link_list.append("https://www.awmf.org/"+ a.get("href"))
        ref_list.append(a.get("title"))
    counter_val = counter_val + 1
    if(counter_val == 1):
        break
print("Hier einmal die Referenzliste:")
print("")
ref_list = list(dict.fromkeys(ref_list))
for d in range(0, len(ref_list)):
    print(ref_list[d])

#ab hier vergleich von themenverwandten LL und Referenzliste
print("")
list_with_duplicates = []
print("Hier der Vergleich zwischen Ergebnisliste ohne Synonyme und Referenzliste")
for i in range(0, len(res_list)):
    for j in range(0, len(ref_list)):
        if(ref_list[j] in res_list[i]):
            list_with_duplicates.append(res_list[i])

list_without_duplicates = list(dict.fromkeys(list_with_duplicates))
print(list_without_duplicates)"""
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! BIS HIERHIN AUSKOMMENTIERT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#ab hier Synonymlisten erstellen
#ICD-10 File einlesen und 2 Listen für die Codes und die Namen erstellen
print("")
print("Jetzt kommt ICD-10-GM !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
fileHandle = open('ICD_10_gm.txt', encoding='utf-8')
#fileHandle = open('test.txt', encoding='utf-8')
icd_10_codes = []
icd_10_names = []

for line in fileHandle:
    fields = line.split('|')
    if(fields[3] == "" and fields[5] != ""):
        icd_10_codes.append(fields[5])
    if(fields[5] == "" and fields[3] != ""):
        icd_10_codes.append(fields[3])
    if(fields[3] == "" and fields[5]==""):
        icd_10_codes.append(" ")
    if(fields[3] != "" and fields[5] != ""):
        icd_10_codes.append(fields[5])
    icd_10_names.append(fields[7].rstrip("\n"))

test_keyword_list = tokens_without_sw

#an dieser Stelle werden die rausgefilterten Wörter in ICD-10-GM geworfen 
# und anschließend werden die ICD-10 Namen in einer Liste gespeichert

def find_syn(list1, names, codes):
    icd_10_names_for_es = []
    for i in range(0, len(list1)):
        if(len(icd_10_names_for_es) >= 10):
            continue
        for j in range(0, len(names)):
            if(list1[i] in names[j]):
                new_value = names.index(names[j])
                icd_10_names_for_es.append(names[new_value])
                for x in range(0,len(codes)):
                    if(codes[x] == codes[new_value]):
                        icd_10_names_for_es.append(names[x].replace("\n", ""))
        
    
    return icd_10_names_for_es

icd_10_names_for_es = find_syn(test_keyword_list, icd_10_names, icd_10_codes)

#löschen der Duplikate in der Liste
icd_10_names_for_es = list(dict.fromkeys(icd_10_names_for_es))
#print(icd_10_names_for_es) 
#funktioniert bis hierhin

#ab hier ICD-10-Liste in Elasticsearch reinpacken und Ergebnisliste erhalten 
#anschließend Ergebnisliste mit Referenzliste vergleichen
icd_syns = icd_10_names_for_es

#hier Funktion für Elasticsearchsuche 
def es_search(list1):
    list2 = []
    for i in range(0, len(list1)):
        icd_res = es.search(index='a1', doc_type='files_a1', 
                    body={
                    "from" : 0, "size" : 1000,
                    "_source":"name",
                    "query":{
                        "match_phrase":{
                            "content":list1[i] 
                        }
                    }
                }
        )
        x_var_icd = len(icd_res['hits']['hits'])
        for j in range(0, x_var_icd):
            list2.append(icd_res['hits']['hits'][j]['_source']['name'])
    return list2

#Synonymliste von ICD-10 in ES-Funktion reinwerfen    
icd_res_list = es_search(icd_syns)

#print(icd_res_list)
icd_res_list = list(dict.fromkeys(icd_res_list))
#ab hier Vergleich von ICD-10-GM-ES-Ergebnisliste mit der Referenzliste
print("Vergleich zwischen ICD-10 Ergebnisiste nach Elasticsearch und Referenzliste")
print("")
"""for i in range(0, len(icd_res_list)):
    for j in range(0, len(ref_list)):
        if(ref_list[j] in icd_res_list[i]):
            print(icd_res_list[i])"""

print("ICD-10 abgehakt !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
#ab hier OPS selbes Prinzip wie bei ICD-10
print("")
print("Jetzt kommt OPS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
fileHandle = open('OPS.txt', encoding='utf-8')
#fileHandle = open('test.txt', encoding='utf-8')
ops_codes = []
ops_names = []

for line in fileHandle:
  fields = line.split('|')
  ops_codes.append(fields[2])
  ops_names.append(fields[4].replace("\n",""))

#ab hier wird OPS-Liste für die Tokens erstellt und anschließend in Elasticsearch reinpacken

#Funktion verwenden, um Synonyme zu finden
ops_names_for_es = find_syn(test_keyword_list, ops_names, ops_codes)

#löschen der Duplikate in der Liste
ops_names_for_es = list(dict.fromkeys(ops_names_for_es)) 
#funktioniert bis hierhin
#ab hier OPS Liste in ES
ops_syns = ops_names_for_es

ops_res_list = es_search(ops_syns)

ops_res_list = list(dict.fromkeys(ops_res_list))
#ab hier Vergleich von OPS-ES-Ergebnisliste mit der Referenzliste
print("Vergleich zwischen OPS Ergebnisiste nach Elasticsearch und Referenzliste")
print("")
"""for i in range(0, len(ops_res_list)):
    for j in range(0, len(ref_list)):
        if(ref_list[j] in ops_res_list[i]):
            print(ops_res_list[i])"""

print("OPS abgehakt !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
print("")
#ab hier LOINC in einer Liste speichern und anschließend in ES einfügen
print("Jetzt kommt LOINC!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
loinc_names = []
loinc_codes = []
fileHandle = open('LOINC.txt', encoding='utf-8')
for line in fileHandle:
    fields = line.split(',')
    loinc_codes.append(fields[0].replace('"',''))
    loinc_names.append(fields[9].replace('"',''))


#ab hier wird OPS-Liste für die Tokens erstellen und anschließend in Elasticsearch reinpacken
loinc_names_for_es = []
for i in range(0, len(test_keyword_list)):
    for j in range(0, len(loinc_names)):
        if(test_keyword_list[i] in loinc_names[j]):
            new_value = loinc_names.index(loinc_names[j])
            loinc_names_for_es.append(loinc_names[new_value])
            #ab hier werden OPS-Namen mit den gleichen Codes ebenfalls zur Liste hinzugefügt
            for x in range(0,len(ops_codes)):
                if(loinc_codes[x] == ops_codes[new_value]):
                    o = loinc_names[x]
                    loinc_names_for_es.append(o)

#löschen der Duplikate in der Liste
loinc_names_for_es = list(dict.fromkeys(loinc_names_for_es)) 
#funktioniert bis hierhin
#ab hier LOINC-Liste in ES
loinc_syns = loinc_names_for_es
loinc_res_list = []
for i in range(0, len(loinc_syns)):
    loinc_res = es.search(index='a1', doc_type='files_a1', 
                body={
                    "from" : 0, "size" : 1000,
                    "_source":"name",
                    "query":{
                        "match_phrase":{
                            "content":loinc_syns[i]
                            
                        }
                    }
                }
    )
    x_var_loinc = len(loinc_res['hits']['hits'])
    for j in range(0, x_var_loinc):
        loinc_res_list.append(loinc_res['hits']['hits'][j]['_source']['name'])

loinc_res_list = list(dict.fromkeys(loinc_res_list))
#ab hier Vergleich von LOINC-ES-Ergebnisliste mit der Referenzliste
print("Vergleich zwischen LOINC Ergebnisiste nach Elasticsearch und Referenzliste")
print("")
"""for i in range(0, len(loinc_res_list)):
    for j in range(0, len(ref_list)):
        if(ref_list[j] in loinc_res_list[i]):
            print(loinc_res_list[i])"""

print("LOINC abgehakt !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

#ab hier Suchbegriff Top-10 bestimmen mit Synonymlisten und Suchwort in Kombination + Elasticsearch
#Schritt 1: Suchbegriff Tokenisieren (Titel in Tokens aufteilen) -> Liste mit Tokens entsteht
suchwort = query_liste[0]
searchwords = tokenizing(suchwort)





#Schritt 2: Tokenisierte Liste in ICD-10, OPS, LOINC und in alle 3 übergeben
icd10_searchwords = find_syn(searchwords, icd_10_names, icd_10_codes)
icd10_searchwords = list(dict.fromkeys(icd10_searchwords))
ops_searchwords = find_syn(searchwords, ops_names, ops_codes)
ops_searchwords = list(dict.fromkeys(ops_searchwords))
loinc_searchwords = find_syn(searchwords, loinc_names, loinc_codes)
loinc_searchwords = list(dict.fromkeys(loinc_searchwords))

#Funktion zum AND-verknüpfen der ursprünglichen Tokens
#dadurch sollen alle Leitlinien abgedeckt werden, die alle Tokens enthalten
def token_string_function(searchwords):
    new_token_string = searchwords[0]
    concat_token_string = new_token_string
    concat_token_string2 = new_token_string
    for x in range(1, len(searchwords)):
        #Tokens werden im String miteinander AND-verknüpft
        new_token_string = new_token_string + " AND " + searchwords[x]
        #Tokens werden im zweiten String miteinander konkateniert
        #concat_token_string = concat_token_string + " " + searchwords[x]
        #concat_token_string2 = concat_token_string2 + ", " + searchwords[x]
    #string1 und string2 werden OR-verknüpft, um die Kombination der Wörter zu beachten
    #einmal mit leerzeichen und einmal mit Komma
    #new_token_string = new_token_string + " OR " + concat_token_string + " OR " + concat_token_string2
    #new_token_string = new_token_string + " AND " + titelwort
    return new_token_string

#Funktion zum OR-verknüpfen der jeweiligen Synonymwörter
def token_string_function_2(syn_searchwords):
    number_list = ["0","1", "2", "3", "4", "5","6","7","8","9","+", "-", "=", "&&", "||", ">", "<", "!", "(", ")", "{", "}", "[", "]", "^", '"', "~", "*", "?", ":", "/"]
    new_token_string = ""
    if(len(syn_searchwords) < 1):
        return new_token_string
    else:
        for p in number_list:
            if p not in syn_searchwords[0]:
                new_token_string = syn_searchwords[0]
        for x in range(1, len(syn_searchwords)):
        #for x in range(1, 10):
            counter_num = 0
            for p in number_list:
                if p not in syn_searchwords[x]:
                    counter_num = counter_num + 1
                    if(counter_num == len(number_list)):
                        new_token_string = new_token_string + " OR " + syn_searchwords[x]
        return new_token_string

titelwort = ""
if "," not in suchwort:
    titelwort = suchwort
    searchword_token_string = suchwort
    print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH")
else:
    searchword_token_string = token_string_function(searchwords)
    print("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")
searchword_token_string = titelwort
if(len(icd10_searchwords) > 0 ):
    icd10_searchword_token_string = searchword_token_string
    #for x in range(0, 10):
    icd10_searchword_token_string = icd10_searchword_token_string + " OR " + token_string_function_2(icd10_searchwords) #icd10_searchwords[x]
else:
    icd10_searchword_token_string = ""
if(len(ops_searchwords) > 0 ):
    ops_searchword_token_string = searchword_token_string
    #for x in range(0, len(ops_searchwords)):
    ops_searchword_token_string = ops_searchword_token_string + " OR " + token_string_function_2(ops_searchwords) #ops_searchwords[x]
else:
    ops_searchword_token_string = ""
if(len(loinc_searchwords) > 0 ):
    loinc_searchword_token_string = searchword_token_string + " OR " + token_string_function_2(loinc_searchwords)
else:
    loinc_searchword_token_string = ""
word1 = icd10_searchword_token_string#token_string_function_2(icd10_searchwords)
word2 = ops_searchword_token_string#token_string_function_2(ops_searchwords)
word3 = token_string_function_2(loinc_searchwords)
#all_searchword_token_string = searchword_token_string + " OR " + icd10_searchword_token_string + " OR " + ops_searchword_token_string + " OR " + loinc_searchword_token_string
all_searchword_token_string = searchword_token_string
if(len(word1) > 0):
    all_searchword_token_string = all_searchword_token_string + " OR " + word1
if(len(word2) > 0):
    all_searchword_token_string = all_searchword_token_string + " OR " + word2
if(len(word3) > 0):
    all_searchword_token_string = all_searchword_token_string + " OR " + word3
#all_searchword_token_string = searchword_token_string + " OR " + word1 + " OR " + word2 + " OR " +  word3
#Schritt 3: alle Ergebisse in einer großen Liste speichern
"""big_list_icd10 = icd10_searchwords
big_list_ops = ops_searchwords
big_list_loinc = loinc_searchwords
big_final_list = []
for h in range(0, len(searchwords)):
    big_list_icd10.append(searchwords[h])
    big_list_ops.append(searchwords[h])
    big_list_loinc.append(searchwords[h])
    big_final_list.append(searchwords[h])
for x in range(0,len(icd10_searchwords)):
    big_final_list.append(icd10_searchwords[x])
for y in range(0, len(ops_searchwords)):
    big_final_list.append(ops_searchwords[y])
for z in range(0, len(loinc_searchwords)):
    big_final_list.append(loinc_searchwords[z])"""
#Schritt 4: große Liste in Elasticsearch packen und Top-10 ausgeben lassen

#neue es_Search funktion
"""new_token_string = searchwords[0]
new_token_string2 = searchwords[0]
for x in range(1, len(searchwords)):
    new_token_string = new_token_string + " AND " + searchwords[x]
    new_token_string2 = new_token_string2 + " OR " + searchwords[x]
    #new_final_token_string = new_token_string[:-4]"""


#new_token_string_full = new_token_string + " OR " + new_token_string2

"""final_result_without_syn = es.search(index='a1', doc_type='files_a1', 
            body={
                "from" : 0, "size" : 100,        #hier habe ich von 1000 auf 100 geändert, um weniger Ergebnisse zu erhalten
                "_source":"name",
                "query":{
                    "query_string":{
                        "fields": ["content"],
                        "query":new_token_string#new_token_string2
                    }
                }
            }
    )"""
#Suchfunktion/Abfrage für die Tokens und Synonyme
def es_search_2(list1):
    list2 = []
    list_new = es.search(index='a1', doc_type='files_a1', 
        body={
            "from" : 0, "size" : 100,        #hier habe ich von 1000 auf 100 geändert, um weniger Ergebnisse zu erhalten
            "_source":"name",
            "query":{
                "query_string":{
                    "fields": ["content"],
                    "query":list1#new_token_string2
                }
            }
        }
    )
    x_var_icd = len(list_new['hits']['hits'])
    for j in range(0, x_var_icd):
        list2.append(list_new['hits']['hits'][j]['_source']['name'])
    return list2

#Elasticsearch Suche mit Tokens ohne Synonyme
final_result_without_syn = es_search_2(searchword_token_string)
#final_result_without_syn = es_search_2(query_liste[0])
final_result_without_syn = list(dict.fromkeys(final_result_without_syn))

#Elasticsearch Suche mit Tokens und ICD-10 
final_result_icd10 = es_search_2(icd10_searchword_token_string)
final_result_icd10 = list(dict.fromkeys(final_result_icd10))

#Elasticsearch Suche mit Tokens mit OPS
final_result_ops = es_search_2(ops_searchword_token_string)
final_result_ops = list(dict.fromkeys(final_result_ops))

#Elasticsearch Suche mit Tokens mit LOINC
final_result_loinc = es_search_2(loinc_searchword_token_string)
final_result_loinc = list(dict.fromkeys(final_result_loinc))

#Elasticsearch Suche mit Tokens mit ICD-10, OPS und LOINC
big_final_list_end = es_search_2(all_searchword_token_string)
big_final_list_end = list(dict.fromkeys(big_final_list_end))
len_val_normal = 0
len_val_icd_10 = 0
len_val_ops = 0
len_val_loinc = 0
#hier Länge auf 10 Ergebnisse festlegen
if(len(final_result_without_syn) > 10):
    len_val_normal = 10
else:
    len_val_normal = len(final_result_without_syn)
if(len(final_result_icd10) > 10):
    len_val_icd_10 = 10
else:
    len_val_icd_10 = len(final_result_icd10)
if(len(final_result_ops) > 10):
    len_val_ops = 10
else:
    len_val_ops = len(final_result_ops)
if(len(final_result_loinc) > 10):
    len_val_loinc = 10
else:
    len_val_loinc = len(final_result_loinc)

#Ausgabe der einzelnen Listen
print("NORMALE LISTE")
for h in range(0, len_val_normal):
    print(final_result_without_syn[h])

print("ICD-10 LISTE")
for i in range(0, len_val_icd_10):
    print(final_result_icd10[i])

print("OPS LISTE")
for k in range(0, len_val_ops):
    print(final_result_ops[k])

print("LOINC LISTE")
for l in range(0, len_val_loinc):
    print(final_result_loinc[l])

print("FINALE LISTE")
for u in range(0, 10):
    print(big_final_list_end[u])
print("blllllllllaaaaaaaaaaaaaaaaaaaaaaaaaaa")
#print(token_string_function_2(icd10_searchwords))
print("bluh")
"""print("hello again\n")
print(ops_searchword_token_string)
print("blalala")
print(ops_searchwords)
print("blala")
print(token_string_function_2(ops_searchwords))
print("buuuh")"""
#print(loinc_names)
#ab hier den durchschnittlichen Prozentsatz über alle themenverwandten Leitlinien ermitteln
#also für jede Leitlinie überprüfen, ob die Referenzliste mit der Ergebnisliste übereinstimmt
#Variante 1: ohne Synonymlisten -> Referenzliste jeder Leitlinie einzeln holen und vergleichen
#Ergebnis soll den prozentualen Anteil der Ergebnisliste in der Referenzliste ermitteln

#Funktion für die Referenzliste
def get_ref_list(value, list1):
    ref_list = []
    ref_link_list = []
    new_seite = requests.get(list1[value])
    new_soup = bs(new_seite.text, "html.parser")
    counter_val = 0
    for data in new_soup.find_all("ul", attrs={"class":"ref-list"}):
        for a in data.find_all("a"):
            ref_link_list.append("https://www.awmf.org/"+ a.get("href"))
            ref_list.append(a.get("title"))
        counter_val = counter_val + 1
        if(counter_val == 1):
            break
    return ref_list

#Schritt 1: Schleife, die alle Leitlinien durchgeht
#Schritt 2: Referenzliste jeder Leitlinie ermitteln

#new_ref_list = get_ref_list(0, neue_liste)
#new_ref_list = list(dict.fromkeys(new_ref_list))
#for d in range(0, len(new_ref_list)):
#    print(new_ref_list[d])

#Schritt 3: Ergebnisliste jeder Leitlinie ermitteln durch ES
percentage_list = []
#new_counter = 0
#gesamte Liste von Titeln durchgehen
#for e in range(0, 400):
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!AB HIER HABE ICH AUSKOMMENTIERT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#for e in range(0, 100):
for e in range(0, len(title_list)):
    if(e == 280 or e == 279):
        continue
    #Referenzliste für den jeweiligen Titel raussuchen 
    new_ref_list = get_ref_list(e, neue_liste)
    new_ref_list = list(dict.fromkeys(new_ref_list))
    if(len(new_ref_list) == 0):
        continue
    #else:
    #    new_counter = new_counter + 1
    #    continue
    #print(new_counter)

    erg_list = []
    list_with_dups = []
    #Tokenizing des gewählten Titels
    token_list = tokenizing(title_list[e])
    #Tokenliste durchgehen und Ergbnis in einer Liste speichern
    #print(find_syn(token_list, icd_10_names, icd_10_codes))
    #ICD-10 verwenden
    new_icd_10_names_for_es_1 = find_syn(token_list, icd_10_names, icd_10_codes)
    new_icd_10_names_for_es_1 = list(dict.fromkeys(new_icd_10_names_for_es_1))
    #OPS verwenden
    new_ops_names_for_es = find_syn(token_list, ops_names, ops_codes)
    new_ops_names_for_es = list(dict.fromkeys(new_ops_names_for_es))
    #new_icd_10_names_for_es = new_ops_names_for_es
    #LOINC verwenden
    new_loinc_names_for_es = find_syn(token_list, loinc_names, loinc_codes)
    new_loinc_names_for_es = list(dict.fromkeys(new_loinc_names_for_es))
    #new_icd_10_names_for_es = new_loinc_names_for_es
    #alle verwenden
    new_list = []
    """for word in range(0, len(token_list)):
        new_list.append(token_list[word])
    for word in range(0, len(new_icd_10_names_for_es_1)):
        new_list.append(new_icd_10_names_for_es_1[word])
    for word in range(0, len(new_ops_names_for_es)):
        new_list.append(new_ops_names_for_es[word])
    for word in range(0, len(new_loinc_names_for_es)):
        new_list.append(new_loinc_names_for_es[word])"""
    
    #hier von jedem die ersten 10
    for word in range(0, len(token_list)):
        new_list.append(token_list[word])
    for word in range(0, 10):
        if(len(new_icd_10_names_for_es_1) > 10):
            new_list.append(new_icd_10_names_for_es_1[word])
        if(len(new_ops_names_for_es) > 10):
            new_list.append(new_ops_names_for_es[word])
        if(len(new_loinc_names_for_es) > 10):
            new_list.append(new_loinc_names_for_es[word])

    
    #hier für ICD-10
    #new_icd_10_names_for_es = new_icd_10_names_for_es_1

    #hier normale liste
    #new_icd_10_names_for_es = token_list

    #hier für OPS
    #new_icd_10_names_for_es = new_ops_names_for_es

    #hier für LOINC
    #new_icd_10_names_for_es = new_loinc_names_for_es

    #hier für ICD-10
    new_icd_10_names_for_es = new_icd_10_names_for_es_1

    #hier new_list um alle Klassifikationen zu testen
    #new_icd_10_names_for_es = new_list

    #hier liste kürzen auf 10
    #print("CHECK")
    if(len(new_icd_10_names_for_es) > 10):
        while len(new_icd_10_names_for_es) > 10:
            new_icd_10_names_for_es.pop()
            #print(len(new_icd_10_names_for_es))
    #print("DONE!!!!!!!!!!!")
    icd_10_tokens = []
    icd_val = 0
    if(len(new_icd_10_names_for_es) > 10):
        icd_val = 10
    else:
        icd_val = len(new_icd_10_names_for_es)
    #for a in range(0, len(new_icd_10_names_for_es)):
    for a in range(0, icd_val):
        raw = tokenizing(new_icd_10_names_for_es[a])
        for b in range(0, len(raw)):
            icd_10_tokens.append(raw[b])
    new_token_string = ""
    #number_list = ["0","1", "2", "3", "4", "5","6","7","8","9","+", "-", "=", "&&", "||", ">", "<", "!", "(", ")", "{", "}", "[", "]", "^", '"', "~", "*", "?", ":", "/"]
    #HIER AUSKOMMENTIERT ZEILE 850 und 852 und 855
    #for x in range(0, len(icd_10_tokens)):
        #if(icd_10_tokens[x] not in number_list):
        #new_token_string = new_token_string + icd_10_tokens[x] + " OR "
    new_token_string = token_string_function_2(new_icd_10_names_for_es)
    new_final_token_string = new_token_string
    #new_final_token_string = new_token_string[:-4]
    new_final_token_string = new_final_token_string.replace("[","")
    new_final_token_string = new_final_token_string.replace("]","")
    new_final_token_string = new_final_token_string.replace("(","")
    new_final_token_string = new_final_token_string.replace(")","")
    new_final_token_string = new_final_token_string.replace("/","")
    new_final_token_string = new_final_token_string.replace(":","")
    new_final_token_string = new_final_token_string.replace(";","")
    new_final_token_string = new_final_token_string.replace("0","")
    new_final_token_string = new_final_token_string.replace("1","")
    new_final_token_string = new_final_token_string.replace("2","")
    new_final_token_string = new_final_token_string.replace("3","")
    new_final_token_string = new_final_token_string.replace("4","")
    new_final_token_string = new_final_token_string.replace("5","")
    new_final_token_string = new_final_token_string.replace("6","")
    new_final_token_string = new_final_token_string.replace("7","")
    new_final_token_string = new_final_token_string.replace("8","")
    new_final_token_string = new_final_token_string.replace("9","")
    bla = new_final_token_string
    new_final_token_string = new_final_token_string.replace("|","")
    new_final_token_string = new_final_token_string.replace("=","")
    new_final_token_string = new_final_token_string.replace("&","")
    new_final_token_string = new_final_token_string.replace("!","")
    new_final_token_string = new_final_token_string.replace("?","")
    new_final_token_string = new_final_token_string.replace("%","")
    new_final_token_string = new_final_token_string.replace("$","")
    new_final_token_string = new_final_token_string.replace("§","")
    new_final_token_string = new_final_token_string.replace("^","")
    new_final_token_string = new_final_token_string.replace("*","")
    new_final_token_string = new_final_token_string.replace("+","")
    new_final_token_string = new_final_token_string.replace("#","")
    new_final_token_string = new_final_token_string.replace("-","")
    new_final_token_string = new_final_token_string.replace("~","")
    new_final_token_string = new_final_token_string.replace("@","")
    new_final_token_string = new_final_token_string.replace("€","")
    new_final_token_string = new_final_token_string.replace("<","")
    new_final_token_string = new_final_token_string.replace(">","")
    new_final_token_string = new_final_token_string.replace("_","")
    new_final_token_string = new_final_token_string.replace(".","")
    new_final_token_string = new_final_token_string.replace("µ","")
    new_final_token_string = new_final_token_string.replace("°","")
    new_final_token_string = new_final_token_string.replace("²","")
    new_final_token_string = new_final_token_string.replace("³","")
    #print(new_final_token_string)
    es_list_for_token = es.search(index='a1', doc_type='files_a1', 
            body={
                "from" : 0, "size" : 100,        #hier habe ich von 1000 auf 100 geändert, um weniger Ergebnisse zu erhalten
                "_source":"name",
                "query":{
                    "query_string":{
                        "fields": ["content"],
                        "query":bla#new_final_token_string
                    }
                }
            }
    )
    print(e)
    test_value = 0
    if(len(es_list_for_token['hits']['hits']) > 10):
        test_value = 10
    else:
        test_value = len(es_list_for_token['hits']['hits'])
    for t in range(0, test_value):
        erg_list.append(es_list_for_token['hits']['hits'][t]['_source']['name'])

    #Vergleich von Ref-List und Erg-List
    for w in range(0, len(erg_list)):
        for c in range(0, len(new_ref_list)):
            if(new_ref_list[c] in erg_list[w]):
                list_with_dups.append(erg_list[w])

    list_without_dups = list(dict.fromkeys(list_with_dups))
    if(len(new_ref_list) > 0):
        new_val = len(list_with_dups) / len(new_ref_list)
        if(new_val > 1.0):
            new_val = 1.0
        percentage_list.append(new_val)
    else:
        #percentage_list.append(0)
        continue



print("Percentage List")
print(percentage_list)
print("Final Percentage")
final_percentage = sum(percentage_list) / len(percentage_list)
print(final_percentage)

print(len(percentage_list))
#!!!!!!!!!!!!! BIS HIER ENTKOMMENTIEREN !!!!!!!!!!!!!!!!!!!!!!!
#Schritt 4: Vergleich der Listen
#Schritt 5: prozentualen Anteil ermitteln und in einer neuen Liste speichern
#Schritt 6: Liste mit prozentualen Anteilen durch die Gesamtlänge teile
#Schritt 7: Ergebnis mit dem Durchschnitt erhalten

#print("new_counter")
#print(new_counter)

"""
print("_______________________________________TESTGEBIET_____________________________________________")
#kleiner Test für ein Beispiel
#Referenzliste für den jeweiligen Titel raussuchen 
new_ref_list = get_ref_list(0, neue_liste)
new_ref_list = list(dict.fromkeys(new_ref_list))
print("Ref_list")
print(new_ref_list)
erg_list = []
list_with_dups = []
#Tokenizing des gewählten Titels
token_list = tokenizing(title_list[0])
print("Tokens")
print(token_list)
new_token_string = ""
for x in range(0, len(token_list)):
    new_token_string = new_token_string + token_list[x] + " OR "
print(new_token_string)
#Tokenliste durchgehen und Ergbnis in einer Liste speichern
#ES-Suche für jeden Token
es_list_for_token = es.search(index='a1', doc_type='files_a1', 
            body={
                "from" : 0, "size" : 1000,
                "_source":"name",
                "query":{
                    "query_string":{
                        "fields": ["content"],
                        "query":new_token_string[:-3]
                    }
                }
            }
)
#print(es_list_for_token)
#ES-Ergebnis durchgehen und einzeln zur Ergebnisliste hinzufügen
for t in range(0, 10):
    erg_list.append(es_list_for_token['hits']['hits'][t]['_source']['name'])
print("Erg-List")
print(erg_list)
#Vergleich von Ref-List und Erg-List
for w in range(0, len(erg_list)):
    for c in range(0, len(new_ref_list)):
        if(new_ref_list[c] in erg_list[w]):
            list_with_dups.append(erg_list[w])
list_without_dups = list(dict.fromkeys(list_with_dups))
print(list_without_dups)"""