import requests
from datetime import datetime
import pandas as pd
import json
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import FieldFilter

def extrair_lat(texto):
    return texto["lat"]

def extrair_lng(texto):
    return texto["lng"]

def limpa_trajeto(trajeto):
    train_steps_list = []

    routes = trajeto["routes"]

    for r in routes:
        steps = r["legs"][0]["steps"]

        train_steps_dict = {}
        train_steps_dict["distance"] = r["legs"][0]["distance"]["text"]
        train_steps_dict["duration"] = r["legs"][0]["duration"]["value"]
        
        train_steps = []
        for i in steps:
            try:
                o = i["transit_details"]
                if o["line"]["vehicle"]["type"] in ("BUS", "SUBWAY", "HEAVY_RAIL"):
                    k = o["departure_stop"]["location"]
                    k["name"] = o["departure_stop"]["name"]
                    k["type"] = o["line"]["vehicle"]["type"]
                    train_steps.append(k)
            except:
                pass
        if train_steps != []:
            train_steps_dict["steps"] = train_steps
            train_steps_list.append(train_steps_dict)
    return train_steps_list

def geolocation(lat, lng):
    return requests.get(f"https://maps.googleapis.com/maps/api/geocode/json?key=AIzaSyBjIHPm6qzjQo_AbGE7xO1SYBch_BsyXho&latlng={lat}, {lng}").json()

def converte(paulista):
    paulista_dict = {}
    
    for i in paulista:
        paulista_dict[i["name"]] = i["data"]
        
    return pd.DataFrame(paulista_dict)


def procura_ponto(y, dados_metro_pd):
    dc = 1000
    cont = 0
    x = geolocation(y[0], y[1])
    for i in x["results"]:
        lat1 = float(int(i["geometry"]["location"]["lat"]*dc))/dc
        lng1 = float(int(i["geometry"]["location"]["lng"]*dc))/dc

        lat2 = float(int(i["geometry"]["viewport"]["northeast"]["lat"]*dc))/dc
        lng2 = float(int(i["geometry"]["viewport"]["northeast"]["lng"]*dc))/dc

        lat3 = float(int(i["geometry"]["viewport"]["southwest"]["lat"]*dc))/dc
        lng3 = float(int(i["geometry"]["viewport"]["southwest"]["lng"]*dc))/dc

        for lat in [lat1, lat2, lat3]:        
            for lng in [lng1, lng2, lng3]:
                if cont == 0:
                    docs = dados_metro_pd.where(filter=FieldFilter("latitude_abr", "==", lat))\
                                        .where(filter=FieldFilter("longitude_abr", "==", lng))\
                                        .stream()#.to_dict()
                    lista = []
                    for doc in docs:
                        lista.append(doc.to_dict())
                        
                    try:
                        df = lista[0]
                        lotacao = df["populartimes"]
                        cont = 1
                    except Exception as e:
                        lotacao = []
                    
    return lotacao


def calcula_lotacao2(y, name, typ, dados_metro_pd, dados3_pd):
    
    if typ == "BUS":
        #print(name)
        docs = dados3_pd.where(filter=FieldFilter("name", "==", name)).stream()
        lista = []
        for doc in docs:
            lista.append(doc.to_dict())
            
        try:
            df = lista[0]["populartimes"]
            lotacao = df            
        except Exception as e:
            lotacao = procura_ponto(y, dados3_pd)    
            
    else:
        lotacao = procura_ponto(y, dados_metro_pd)
        
    try:
        lotacao_list = []
        for i in lotacao.keys():
            lotacao_list.append(lotacao[i])
            #print(lotacao_list)
        tabela_lotacoes = converte(lotacao_list)#converte(next(iter(dict(lotacao_list).values()))).reset_index()
        lotacao_agora = tabela_lotacoes[datetime.now().strftime("%A")].to_list()
    except:
        pass
    try:
        return lotacao_agora, name
    except:
        return [], name
    

    
def convert_seconds(duration):
    hours = duration // 3600
    minutes = (duration % 3600) // 60
    seconds = duration % 60

    readable_duration = f"{hours:02}:{minutes:02}"
    
    return readable_duration

def pega_linhas(base):
    rotas = []
    for i in base["routes"]:
        linhas = []
        for j in i["legs"][0]["steps"]:
            try:
                if j["transit_details"]["line"]["vehicle"]["type"] in ("BUS", "SUBWAY", "HEAVY_RAIL"):
                    linhas.append(j["transit_details"]["line"]["short_name"])
            except:
                pass
        rotas.append(linhas)
    return rotas

def retorna_report(db, valor):
    
    try:
        if valor[0:5]=="Linha":
            docs = db.collection('alertas_metro').where(filter=FieldFilter("Status", "not-in", ["Operação Encerrada", "Operações Encerradas"]))\
                                        .where(filter=FieldFilter("Linha", "==", valor)).stream()
            lista = []
            for doc in docs:
                lista.append(doc.to_dict())
                
            linha = lista[0]
            try:
                return {"Linha":valor, "Data":linha["data"], "Status":linha["Status"]}
            except:
                return f""
        else:            
            docs = db.collection('alertas_sptrans').where(filter=FieldFilter("Linha", "==", valor)).stream()
            lista = []
            for doc in docs:
                lista.append(doc.to_dict())
                
            linha = lista[0]
            #linha = sptrans[sptrans["Linha"]==valor]
            try:
                return {"Linha":valor, "Descrição":linha['descricao'], "Horário":linha['data_lista'], "Ida":linha['ida'], "Volta":linha['volta'], "Motivo": linha['motivo']}
                #return f"{valor}\n{linha.iloc[0,0]}\n{linha.iloc[0,2]}\n{linha.iloc[0,5]}\n{linha.iloc[0,6]}\n{linha.iloc[0,7]}" 
            except:
                return f""
    except:
        return f""

    
def calcula_hora(row):
    row2 = row["index"]+1
    if row2==24:
        row2=0
    return row2

def calcula_indice(base):
    base["min"] = base.idxmin(axis=1)
    base = base.reset_index()
    base["Hora"] = base.apply(calcula_hora, axis=1)
    base = base.drop(columns="index")
    indice = base[base["Hora"]==datetime.now().hour].reset_index().loc[0, 'min']
    return int(indice[-1])