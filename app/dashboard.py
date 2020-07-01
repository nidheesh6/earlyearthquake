import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import psycopg2
import json
import pandas as pd
import time


app = dash.Dash(__name__)
#app.css.config.serve_locally=False
#app.css.append_css(
#    {'external_url': 'https://codepen.io/amyoshino/pen/jzXypZ.css'})

conn = psycopg2.connect(host='ec2-18-232-24-132.compute-1.amazonaws.com',database='earthquake', user='postgres', password='********')
cur = conn.cursor()


location = pd.read_csv("data_file.csv")

location=location.astype(str)

app.layout = html.Div([
    html.Div([
        html.Div([
            dcc.Graph(id='graph', style={'margin-top': '20'})], className="six columns"),
        html.Div([
            dcc.Graph(
                id='bar-graph'
            )
        ], className='twelve columns'
        ),
        dcc.Interval(
            id='interval-component',
            interval=5*1000,  # in milliseconds
            n_intervals=0)
    ], className="row")

], className="ten columns offset-by-one")


@app.callback(Output('graph', 'figure'), [Input('interval-component', 'n_intervals')])
def update_map(n):
    """
    Args n: int
    :rtype: dict
    """
    try:
        latest_reading = "select * from ereadings limit 90;"
        df_map = pd.read_sql(latest_reading, conn)
        map_data = df_map.merge(location, how='left', left_on=["device_id", "country_code"], right_on=["device_id","country_code"])
        clrred = 'rgb(222,0,0)'
        clrgrn = 'rgb(0,222,0)'

        def SetColor(gal):
            if gal >= .17:
                return clrred
            else:
                return clrgrn

        layout = {
            'autosize': True,
            'height': 500,
            'font': dict(color="#191A1A"),
            'titlefont': dict(color="#191A1A", size='18'),
            'margin': {
                'l': 35,
                'r': 35,
                'b': 35,
                't': 45
            },
            'hovermode': "closest",
            'plot_bgcolor': '#fffcfc',
            'paper_bgcolor': '#fffcfc',
            'showlegend': False,
            'legend': dict(font=dict(size=10), orientation='h', x=0, y=1),
            'name': map_data['country_code'],
            'title': 'earthquake  activity for the last 3 seconds',
            'mapbox': {
                'accesstoken':'*********************************',
                'center': {
                    'lon':-98.49,
                    'lat':18.29


                },
                'zoom': 5,
                'style': "dark"
            }
        }

        return {
            "data": [{
                "type": "scattermapbox",
                "lat": list(location['latitude']),
                "lon": list(location['longitude']),
                "hoverinfo": "text",
                "hovertext": [["sensor_id: {}  <br>country_code: {} <br>gal: {} <br>x: {} <br>y: {}".format(i, j, k, l, m)]
                              for i, j, k, l, m in zip(location['device_id'],location['country_code'].tolist(),map_data['gal'].tolist(),map_data['avg_x'].tolist(), map_data['avg_y'].tolist())],
                "mode": "markers",
                "marker": {
                    "size": 10,
                    "opacity": 1,
                    "color": list(map(SetColor, map_data['gal']))
                }
            }],
            "layout": layout
        }
    except Exception as e:
        print("Error: Couldn't update map")
        print(e)
if __name__ == '__main__':
    app.run_server(debug=False)
