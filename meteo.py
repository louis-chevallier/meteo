import urllib.request
from utillc import *
import json, os
import datetime
import pandas as pd
from io import StringIO, BytesIO
import numpy as np
import pickle

print_everything()

token = "eyJ4NXQiOiJZV0kxTTJZNE1qWTNOemsyTkRZeU5XTTRPV014TXpjek1UVmhNbU14T1RSa09ETXlOVEE0Tnc9PSIsImtpZCI6ImdhdGV3YXlfY2VydGlmaWNhdGVfYWxpYXMiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJsb3Vpcy5jaGV2YWxsaWVyQGNhcmJvbi5zdXBlciIsImFwcGxpY2F0aW9uIjp7Im93bmVyIjoibG91aXMuY2hldmFsbGllciIsInRpZXJRdW90YVR5cGUiOm51bGwsInRpZXIiOiJVbmxpbWl0ZWQiLCJuYW1lIjoiRGVmYXVsdEFwcGxpY2F0aW9uIiwiaWQiOjI3Njc5LCJ1dWlkIjoiN2Q2OWI0MWYtMWQxYy00Mjc2LWIyMTUtNThmMjJjYTk0NzE1In0sImlzcyI6Imh0dHBzOlwvXC9wb3J0YWlsLWFwaS5tZXRlb2ZyYW5jZS5mcjo0NDNcL29hdXRoMlwvdG9rZW4iLCJ0aWVySW5mbyI6eyI1MFBlck1pbiI6eyJ0aWVyUXVvdGFUeXBlIjoicmVxdWVzdENvdW50IiwiZ3JhcGhRTE1heENvbXBsZXhpdHkiOjAsImdyYXBoUUxNYXhEZXB0aCI6MCwic3RvcE9uUXVvdGFSZWFjaCI6dHJ1ZSwic3Bpa2VBcnJlc3RMaW1pdCI6MCwic3Bpa2VBcnJlc3RVbml0Ijoic2VjIn19LCJrZXl0eXBlIjoiUFJPRFVDVElPTiIsInN1YnNjcmliZWRBUElzIjpbeyJzdWJzY3JpYmVyVGVuYW50RG9tYWluIjoiY2FyYm9uLnN1cGVyIiwibmFtZSI6IkRvbm5lZXNQdWJsaXF1ZXNDbGltYXRvbG9naWUiLCJjb250ZXh0IjoiXC9wdWJsaWNcL0RQQ2xpbVwvdjEiLCJwdWJsaXNoZXIiOiJhZG1pbl9tZiIsInZlcnNpb24iOiJ2MSIsInN1YnNjcmlwdGlvblRpZXIiOiI1MFBlck1pbiJ9XSwiZXhwIjoxODQxNDQ5Njc5LCJ0b2tlbl90eXBlIjoiYXBpS2V5IiwiaWF0IjoxNzQ2Nzc2ODc5LCJqdGkiOiI2Y2RjNzkyOC1jYTQwLTQzMTUtYjcyOC1hZGM2YjllMWY5MDkifQ==.tINFKKxJRulfy5Vwh9AqZE_hNqAjKeeExRnUeRFcuIRYLsqm9P9O46noDN69xH0dPgWhF7s3ux6B0zLABv2qoCT85o_hxtdG1dutSsB5hsyW62yPU2OklewI_eT4lwo6szlRa9nJfvug2pw4EFjPO5vTl9n9aZjjA0CBdnHI5wNP_3GNvSPKOBgacHVCjH4kyrbMmktiNTr5swUmwNSAcAQByg_GCzcpqO6x-Fw4xnSMt61lqyxBko7uy8YytzGW1qxNe9-7zbPb9x1QK4cgkx_8wGzEXnpKnBBjjBNYO94Llcsmxf_jsYnIdqfUoTxDItOgmnvEY5wRJ5jPU6C-8Q=="
server = "https://public-api.meteofrance.fr/public/DPClim/v1"

def get_stations_de_dept(
    command = "liste-stations/quotidienne",
#    command = "liste-stations/infrahoraire-6m",
    p="precipitation",
    departement=35) :
  url = server + '/' + command + "?" + "id-departement=%d" % departement
  url += "&" + "parametre=%s" % p
  #EKOX(url)
  req = urllib.request.Request(url)
  req.add_header('apikey', token)
  #EKOX(req)
  response = urllib.request.urlopen(req)
  rep = response.read()
  j=json.loads(rep)
  return j


make_date = lambda yy, mm, dd : datetime.date(yy, mm, dd).isoformat() + "T00:00:00Z"

def get_données(
        deb=make_date(2022, 1, 1),
        fin = make_date(2022, 1, 1),
        station_id=0) :
  command = "commande-station/quotidienne"
  url = server + '/' + command + "?"
  url += "id-station=%s" % str(station_id)
  now = datetime.datetime.now()

  
  url += "&date-deb-periode=%s" % deb
  url += "&date-fin-periode=%s" % fin
  #EKOX(url)
  #EKOX(url)
  req = urllib.request.Request(url)
  req.add_header('apikey', token)
  #EKOX(req)
  response = urllib.request.urlopen(req)
  rep = response.read()
  j=json.loads(rep)

  return j



def get_fichier(ref) :
  command = "commande/fichier"
  url = server + '/' + command + "?"
  url += "id-cmde=%s" % ref
  #EKOX(url)
  req = urllib.request.Request(url)
  req.add_header('apikey', token)
  #EKOX(req)
  response = urllib.request.urlopen(req)
  rep = response.read()
  #EKOX(rep)
  df = pd.read_csv(BytesIO(rep), sep=";", decimal=",")
  return df


## desc des champs
def lit_desc() :
    desc = "https://donneespubliques.meteofrance.fr/client/document/mensq_descriptif_champs_323.csv"
    req = urllib.request.Request(desc)
    response = urllib.request.urlopen(req)
    rep = response.read()
    df = pd.read_csv(BytesIO(rep), sep=":", decimal=",")
    dico = dict([ (r.iloc[0].strip(), str(r.iloc[1]).strip()) for r in df.iloc ])
    dico['POSTE'] = 1
    dico['DATE'] = 2
    return dico
#np.asarray(data['RR'])


dico = lit_desc()
#EKOX(dico.keys())

ps = ['precipitation', 'temperature', 'humidite', 'vent', 'pression', 'rayonnement']
ps = ['rayonnement']


def get_mesures(champ='RR',
                deb = make_date(2022, 1, 1),
                fin = make_date(2022, 7, 1),
                nom='RENNES-ST JACQUES') :
    root = "/content/gdrive/MyDrive/data/meteo"
    root = "/mnt/hd3/data/meteo"
    
    fn = f"{root}/{nom}_{deb}_{fin}.pkl"
    if os.path.exists(fn) :
        data = pd.read_pickle(fn)
    else :
        p='rayonnement' # avec cette mesure on selectionne les stations qui donnent tout
        j=get_stations_de_dept(p=p)
        EKOX(len(j))
        ouverts= [ e for e in j if e["posteOuvert"] == True ]
        sel = [ e for e in j if e["posteOuvert"] == True and e['nom'] == nom]
        EKOX(len(ouverts))
        EKOX(len(sel))    
        #EKOX([e['nom'] for e in ouverts])
        s0 = sel[0]
        EKOX(s0['nom'])
        #"e['nom'] == 'RENNES']
        ref = get_données(deb, fin, station_id = s0['id'])
        EKOX(ref)
        data = get_fichier(ref['elaboreProduitAvecDemandeResponse']['return'])
        #EKOX(data)
        #EKOX(data.head())
        data.to_pickle(fn)
        
        bons_champs = [ series_name for series_name, series in data.items() if not np.isnan(np.min(np.asarray(series)))]
        
    ret = np.asarray(data[champ])
    EKOX(ret.shape)
    EKOX(data.shape)
    return ret


get_mesures(deb=make_date(1992, 1, 1),
            fin=make_date(1993, 1, 1))
            
rr = [ get_mesures(deb=make_date(y, 1, 1), fin=make_date(y, 1, 1)) for y in range(1992, 2002)]
  




for p in ps :
    try :
        EKOX(p)
        j=get_stations_de_dept(p=p)
        EKOX(len(j))
        ouverts= [ e for e in j if e["posteOuvert"] == True]
        EKOX(len(ouverts))
        EKOX([e['nom'] for e in ouverts])
        s0 = ouverts[0]
        EKOX(s0['nom'])
        #"e['nom'] == 'RENNES']
        ref = get_données(s0['id'])
        #EKOX(ref)
        data = get_fichier(ref['elaboreProduitAvecDemandeResponse']['return'])
        #EKOX(data)
        #EKOX(data.head())
        
        EKOX([ series_name for series_name, series in data.items() if not np.isnan(np.min(np.asarray(series)))])
        
        bons_champs = [ series_name for series_name, series in data.items() if not np.isnan(np.min(np.asarray(series)))]
        
        #EKOX( [ dico[e] for e in bons_champs])
        EKOX(data.shape)
    except :
        pass
