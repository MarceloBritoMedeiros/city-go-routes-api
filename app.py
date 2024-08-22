from flask import Flask, request
import requests
from datetime import datetime
import pandas as pd
import json as json_f
from routes_functions import *
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import FieldFilter
cred = credentials.Certificate(r'A:\Projects\TCC\keys\city-go-419101-firebase-adminsdk-chy5n-729ff135d8.json')
firebase_admin.initialize_app(cred)

db = firestore.client()

app = Flask(__name__)

@app.route('/json')
def json():
    transit_mode = request.args.get('transit_mode')
    destination = request.args.get('destination')
    origin = request.args.get('origin')
    key = request.args.get('key')
    mode = request.args.get('mode')
    alternatives = request.args.get('alternatives')
    language = request.args.get('language')
    
    # x = busca_rota_maps("Shopping Cidade São Paulo", "Estrada dos Mirandas 210, Jardim Maria Duarte")
    url = f"https://maps.googleapis.com/maps/api/directions/json?destination={destination}&transit_mode={transit_mode}&origin={origin}&key={key}&mode={mode}&alternatives={alternatives}&language={language}"
    trajeto = requests.get(url).json()        
       
    train_steps_list = limpa_trajeto(trajeto)

    routes = []
    for j in train_steps_list:
        steps = []
        steps2 = []
        for i in j["steps"]:
            result = calcula_lotacao2(geolocation(i['lat'], i['lng']), i["name"], i["type"], db.collection('popular-times-metro'), db.collection('popular-times'))
            steps.append(result[0])
            steps2.append(result[1])
        routes.append((steps2, steps, j["duration"]))

    oo = []
    for j in routes:
        o = {}
        c = 0
        for name, i in zip(j[0], j[1]):
            if i==[]:
                o[name] = j[1][c-1]
            else:
                o[name] = i
            c+=1
        o_pd = pd.DataFrame(o)
        o_pd["media"] = o_pd.mean(axis=1)
        o_pd = o_pd.round(2).reset_index()
        o_pd["hora"] = o_pd["index"]+1
        o_pd = o_pd.drop(columns=['index'])
        o_pd["Segundos de Viagem"] = j[2]
        o_pd["Horas de Viagem"] = o_pd["Segundos de Viagem"].apply(convert_seconds)
        o_pd["duration_in_seconds"] = (o_pd["hora"]*3600) + j[2]
        o_pd['Horário de chegada'] = o_pd['duration_in_seconds'].apply(convert_seconds)

        oo.append(o_pd)
        
    base2 = pega_linhas(trajeto)

    # print(base2)

    avisos = []
    alertas = []
    for i in base2:
        for k in i:
            if k!="":
                alertas.append(retorna_report(k))
        avisos.append(alertas)

    rota = pd.DataFrame()
    tempo = pd.DataFrame()
    lotacao = pd.DataFrame()

    for c, i in enumerate(oo):
        e = i[["media", "Segundos de Viagem"]]
        e = e.rename(columns={'media': f'media_{c}', 'Segundos de Viagem': f'tempo_{c}'})
        e[f"rota_{c}"] = e[f"media_{c}"]*e[f'tempo_{c}']
        
        rota = pd.concat([rota, e[f"rota_{c}"]], axis=1)
        lotacao = pd.concat([lotacao, e[f"media_{c}"]], axis=1)
        tempo = pd.concat([tempo, e[f'tempo_{c}']], axis=1)
        
        
    rota = pd.DataFrame()
    tempo = pd.DataFrame()
    lotacao = pd.DataFrame()

    for c, i in enumerate(oo):
        e = i[["media", "Segundos de Viagem"]]
        e = e.rename(columns={'media': f'media_{c}', 'Segundos de Viagem': f'tempo_{c}'})
        e[f"rota_{c}"] = e[f"media_{c}"]*e[f'tempo_{c}']
        
        rota = pd.concat([rota, e[f"rota_{c}"]], axis=1)
        lotacao = pd.concat([lotacao, e[f"media_{c}"]], axis=1)
        tempo = pd.concat([tempo, e[f'tempo_{c}']], axis=1)

    oo2 = []
    for i in oo:
        oo2.append(i.drop(columns=['Segundos de Viagem', "media", "hora", "Horas de Viagem", "duration_in_seconds", "Horário de chegada"]).to_dict())

    rota_indice = calcula_indice(rota)
    tempo_indice = calcula_indice(tempo)
    lotacao_indice = calcula_indice(lotacao)

    for c, (i, lot, av) in enumerate(zip(trajeto["routes"], oo2, avisos)):
        i["lotacao"] = lot
        i["alertas"] = av
        if c == rota_indice:
            i["classifica"] = "melhor_rota"
        elif c == tempo_indice:
            i["classifica"] = "melhor_tempo"
        elif c == lotacao_indice:
            i["classifica"] = "melhor_lotacao"
        else:
            i["classifica"] = "comum"
        
    return trajeto

if __name__ == '__main__':
    app.run(debug=True)

