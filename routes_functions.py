import requests
from datetime import datetime
import pandas as pd
import json

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

def calcula_lotacao2(x, name, typ, dados_metro_pd, dados3_pd):    
    # print(name)
    if typ == "BUS":
        df = dados3_pd[dados3_pd["name"]==name]
        if df.empty==False:
            lotacao = df["populartimes"]
    else:
        dc = 1000
        cont = 0

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
                        #print(f"{lat}, {lng}")
                        df = dados_metro_pd[(dados_metro_pd["latitude_abr"]==lat)&(dados_metro_pd["longitude_abr"]==lng)]
                        if df.empty==False:
                            lotacao = df["populartimes"]
                            cont = 1
    try:
        tabela_lotacoes = converte(next(iter(dict(lotacao).values()))).reset_index()
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

def retorna_report(valor):
    sptrans = pd.read_excel(r"A:\Projects\TCC\bases_finais\base_sptrans_full_final.xlsx")
    metro = pd.read_excel(r"A:\Projects\TCC\bases_finais\log_metro_final.xlsx")   
    try:
        if valor[0:5]=="Linha":
            metro2 = metro[~metro["Status"].isin(["Operação Encerrada", "Operações Encerradas"])]
            linha = metro2[metro2["Linha"]==valor]
            try:
                return {"Linha":valor, "Data":linha.iloc[0,8], "Status":linha.iloc[0,5]}
            except:
                return f""
        else:
            linha = sptrans[sptrans["Linha"]==valor]
            try:
                return {"Linha":valor, "Descrição":linha.iloc[0,0], "Horário":linha.iloc[0,2], "Ida":linha.iloc[0,5], "Volta":linha.iloc[0,6], "Motivo": linha.iloc[0,7]}
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