"""Create dashboard using environment credentials. No credentials in source or output."""
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen

ROOT=Path(__file__).resolve().parent
base=os.environ.get('METABASE_URL','http://127.0.0.1:3030').rstrip('/')
session=None
def api(method,path,body=None):
    headers={'Content-Type':'application/json'}
    if session:headers['X-Metabase-Session']=session
    request=Request(base+path,method=method,headers=headers,
                    data=None if body is None else json.dumps(body).encode())
    with urlopen(request,timeout=180) as response:
        data=response.read()
        return json.loads(data) if data else None

def main():
    global session
    session=api('POST','/api/session',{'username':os.environ['MB_ADMIN_EMAIL'],'password':os.environ['MB_ADMIN_PASSWORD']})['id']
    statepath=ROOT/'data/metabase-state.json'
    state=json.loads(statepath.read_text()) if statepath.exists() else {}
    def save():statepath.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding='utf-8')
    if 'database_id' not in state:
        state['database_id']=api('POST','/api/database',{'name':'CityBikes · ClickHouse','engine':'clickhouse',
            'details':{'host':os.environ.get('MB_CH_HOST','127.0.0.1'),'port':int(os.environ.get('MB_CH_PORT','8124')),
                       'user':'citybikes_reader','password':os.environ['CH_READER_PASSWORD'],
                       'dbname':'citybikes','enable-multiple-db':False,'ssl':False},
            'is_full_sync':False,'auto_run_queries':True})['id'];save()
    if 'collection_id' not in state:
        state['collection_id']=api('POST','/api/collection',{'name':'CityBikes · диплом инженера данных','description':'Учебный снимок CityBikes, май 2025'})['id'];save()
    if 'dashboard_id' not in state:
        state['dashboard_id']=api('POST','/api/dashboard',{
            'name':'CityBikes · сети и доступность велосипедов',
            'description':'Снимок 12.05.2025. Каталог: 781 сеть; станции загружены только для 3 сетей. Это доступность на момент снимка, не число поездок и не популярность. Пропуски не считаются нулями.',
            'collection_id':state['collection_id'],'parameters':[]})['id'];save()
    cards=[
      ('Сетей в каталоге','scalar','SELECT uniqExact(network_id) AS networks FROM citybikes.dm',{},0,0,6,4),
      ('Сетей с данными станций','scalar','SELECT uniqExactIf(network_id,station_id IS NOT NULL) AS covered_networks FROM citybikes.dm',{},0,6,6,4),
      ('Станций в выборке','scalar','SELECT countIf(station_id IS NOT NULL) AS stations FROM citybikes.dm',{},0,12,6,4),
      ('Доступных велосипедов','scalar','SELECT sum(free_bikes) AS available_bikes FROM citybikes.dm',{},0,18,6,4),
      ('Станции по городам · только 3 сети','bar',"SELECT if(city='Bournemouth, Christchurch and Poole','BCP (UK)',city) AS city_label,count() AS stations FROM citybikes.dm WHERE station_id IS NOT NULL GROUP BY city_label ORDER BY stations DESC",{'graph.dimensions':['city_label'],'graph.metrics':['stations']},4,0,12,7),
      ('Доступные велосипеды по городам','bar',"SELECT if(city='Bournemouth, Christchurch and Poole','BCP (UK)',city) AS city_label,sum(free_bikes) AS available_bikes FROM citybikes.dm WHERE station_id IS NOT NULL GROUP BY city_label ORDER BY available_bikes DESC",{'graph.dimensions':['city_label'],'graph.metrics':['available_bikes']},4,12,12,7),
      ('Сети по странам · весь каталог, топ-12','bar','SELECT country,uniqExact(network_id) AS networks FROM citybikes.dm GROUP BY country ORDER BY networks DESC,country LIMIT 12',{'graph.dimensions':['country'],'graph.metrics':['networks']},11,0,12,7),
      ('Способы оплаты · только известные','bar','SELECT method,count() AS stations FROM citybikes.dm ARRAY JOIN arrayDistinct(payment) AS method WHERE station_id IS NOT NULL GROUP BY method ORDER BY stations DESC,method',{'graph.dimensions':['method'],'graph.metrics':['stations']},11,12,12,7),
      ('Наличие электровелосипедов · данные и пропуски','table',"SELECT multiIf(has_ebikes=1,'Подтверждено',has_ebikes=0,'Нет по источнику','Неизвестно') AS status,count() AS stations FROM citybikes.dm WHERE station_id IS NOT NULL GROUP BY status ORDER BY stations DESC",{},18,0,12,6),
      ('Качество и покрытие снимка','table','SELECT network_name,count() AS stations,countIf(free_bikes=0) AS zero_bikes,countIf(length(payment)>0) AS payment_known,countIf(ebikes IS NOT NULL) AS ebikes_known,min(observed_at) AS snapshot_utc FROM citybikes.dm WHERE station_id IS NOT NULL GROUP BY network_name ORDER BY stations DESC',{},18,12,12,6),
      ('Станции: доступность на момент снимка','table','SELECT city,station_name,free_bikes,empty_slots,ebikes,slots,renting,returning,last_updated FROM citybikes.dm WHERE station_id IS NOT NULL ORDER BY city,station_name',{},24,0,24,9)
    ]
    state.setdefault('cards',{})
    dashcards=[{'id':-100,'card_id':None,'row':0,'col':0,'size_x':24,'size_y':2,
        'visualization_settings':{'text':'**Учебный снимок · 12 мая 2025.** Каталог: 781 сеть. Данные станций: только 3 сети. Показана доступность, не поездки и не популярность. BCP — Bournemouth, Christchurch and Poole.',
                                  'virtual_card':{'display':'text','visualization_settings':{}}}}]
    evidence=[]
    for name,display,sql,viz,row,col,width,height in cards:
        if display=='bar':
            viz.update({'graph.x_axis.title_text':'Город' if 'city_label' in viz['graph.dimensions'] else ('Страна' if 'country' in viz['graph.dimensions'] else 'Способ оплаты'), 'graph.y_axis.title_text':'Количество'})
        body={'name':name,'display':display,'collection_id':state['collection_id'],'visualization_settings':viz,
              'dataset_query':{'database':state['database_id'],'type':'native','native':{'query':sql,'template-tags':{}}}}
        if name in state['cards']:card=api('PUT',f"/api/card/{state['cards'][name]}",body)
        else:
            card=api('POST','/api/card',body);state['cards'][name]=card['id'];save()
        result=api('POST',f"/api/card/{card['id']}/query",{})
        if result.get('status')!='completed':raise RuntimeError('Dashboard query failed: '+name)
        evidence.append({'card':name,'rows':result.get('row_count',len(result['data']['rows'])),'status':result['status']})
        dashcards.append({'id':-len(dashcards)-1,'card_id':card['id'],'row':row+2,'col':col,'size_x':width,'size_y':height,'parameter_mappings':[]})
    api('PUT',f"/api/dashboard/{state['dashboard_id']}",{'dashcards':dashcards,'parameters':[],'width':'full'})
    link=api('POST',f"/api/dashboard/{state['dashboard_id']}/public_link")
    public_base=os.environ.get('METABASE_PUBLIC_URL',base).rstrip('/')
    result={'dashboard_id':state['dashboard_id'],'url':public_base+'/public/dashboard/'+link['uuid'],'cards_verified':evidence}
    (ROOT/'evidence').mkdir(exist_ok=True)
    (ROOT/'evidence/dashboard.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print('Dashboard:',result['url'],'Cards:',len(cards))

if __name__=='__main__':main()
