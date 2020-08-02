# External modules required:
#   pip install dash            --> add dash board elements on web page
#   pip isntall dash_daq        --> add dash DAQ, Data Acquisition and Control
#   pip install python-dotenv   --> use environment variables for email account
#   pip install boto            --> use environment vairables for heroku.com
#   pip install paho-mqtt       --> use MQTT
#   pip install pandas          --> read data

# MQTT import
import paho.mqtt.client as mqtt

# For getting environmental variables
from dotenv import load_dotenv
import os
import urllib.parse
# from boto.s3.connection import S3Connection  # For heroku.com

# For data calculations
import pandas as pd

# For timezone calculations
from pytz import timezone
tz_hk = timezone("Asia/Hong_Kong")
tz_utc = timezone("UTC")

# For file path handling
from pathlib import Path

# *** dash import and set up ***
#
import dash
import dash_daq as daq
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import dash_bootstrap_components as dbc

dash_web_page_update_interval = 5000  # in milliseconds
external_stylesheets = [dbc.themes.BOOTSTRAP]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
# For different themes, visit
# https://dash-bootstrap-components.opensource.faculty.ai/docs/themes/
# For layout uisng dbc, visit
# https://dash-bootstrap-components.opensource.faculty.ai/docs/components/layout/
# *** dash import and set up ***

server = app.server

# *** Read data for graph ***
#
equipment_ID = "gogclpba/T_SKF/C003/NPB19F/RND/Level-Burner"
csv_path = Path("data", "log_processed.csv")
usage_hist_df = pd.read_csv(csv_path, parse_dates=["time"])
# usage_hist_df["time"] = usage_hist_df["time"].dt.tz_localize(tz=tz_hk)
usage_hist_from = usage_hist_df["time"].min()
usage_hist_to = usage_hist_df["time"].max()

usage_showrange = "1 hour"
usage_hist_filter = (usage_hist_df["time"] >
                    (usage_hist_to - pd.Timedelta(usage_showrange)))
usage_hist_extract = usage_hist_df[usage_hist_filter][["time", "Indicator"]]

parts = 5  # No of divisions in dash RangeSlider
dtime = usage_hist_to - usage_hist_from
usage_hist_tlist = [(usage_hist_from + i*dtime/parts) for i in range(parts)]
usage_hist_tlist.append(usage_hist_to)

slider_marks = {(usage_hist_tlist[n]-usage_hist_from).days:
                usage_hist_tlist[n].strftime("%Y-%m-%d")
                for n in range(parts)}
slider_marks[dtime.days] = usage_hist_to.strftime("%Y-%m-%d")
#
# *** Read data for graph ***


# *** MQTT setup ***
#
load_dotenv()
url_str = os.getenv("CLOUDMQTT_URL", default="mqtt://localhost:1883")

# if url_str == None:  # try boto S3Connection for Heroku platform
#     mqtt_dict = S3Connection(os.getenv("CLOUDMQTT_URL"))
#     url_str = mqtt_dict["CLOUDMQTT_URL"]
url = urllib.parse.urlparse(url_str)
mqtt_dict = {
    "MQTT_BROKER": url.hostname,
    "MQTT_USER": url.username,
    "MQTT_PWD":url.password,
    "MQTT_PORT": url.port
}

connect_code = "Nothing"
broker_address = mqtt_dict["MQTT_BROKER"]  # Broker address
port = int(mqtt_dict["MQTT_PORT"])  # Broker port
user = mqtt_dict["MQTT_USER"]  # Connection username
password = mqtt_dict["MQTT_PWD"]  # Connection password
topic_levels = [user,
                "T_SKF",
                "C003",
                "NPB19F",
                ["LED1", "LED2", "LED3", "ANA", "RND"],
                ["Status", "Level-Burner", "Switch"]
]
topic_prefix = "/".join(topic_levels[: 4]) +"/"
topic_subscribe = topic_prefix + "#"
topic_publish = topic_prefix + "LED3/Switch"

# create topic_msg dict with keys=topics
topic_msg = {}
topic_msg[topic_prefix + "LED1/Status"] = "0"
topic_msg[topic_prefix + "LED2/Status"] = "0"
topic_msg[topic_prefix + "LED3/Status"] = "0"
topic_msg[topic_prefix + "ANA/Level-Burner"] = "0"
topic_msg[topic_prefix + "RND/Level-Burner"] = "0"
topic_msg[topic_publish] = 0
#
# *** MQTT setup ***


indicator_colors = {
    "off":   "#a9a9a9",  # grey
    "on_g":  "#7cfc00",  # green
    "on_r":  "#df2020",  # dim red
    "on_y":  "#ffff00",  # yellow
    "fault": "#ff0000"  # red
}

# Label on button for LED3
LED3_btn_lbl = ""

# *** dash web page set up
#
app.layout = html.Div([
    html.Div([
        html.Small(id="debug_txt"),
        html.Hr()
    ]),

    dbc.Row([
        dbc.Col([
            daq.Indicator(
                id="LED1",
                label="LED1",
                labelPosition="bottom",
                size="20",
                color=indicator_colors["off"],
            ),
        ], className="mt2 mb-2", sm=4),

        dbc.Col([
            daq.Indicator(
                id="LED2",
                label="LED2",
                labelPosition="bottom",
                size="20",
                color=indicator_colors["off"],
            ),
        ], className="mt-2 mb-2", sm=4),


        dbc.Col([
            dbc.Row([
                daq.Indicator(
                    id="LED3",
                    # label="LED3",
                    # labelPosition="top",
                    size="30",
                    color=indicator_colors["off"]),
                dbc.Button(
                    children=LED3_btn_lbl,
                    outline=False,
                    id="LED3_switch",
                    color="primary",
                    size="sm",
                    style = {"marginLeft": 5})
            ], justify="center"),
        ], className="mt-2 mb-4", sm=4, align="center"),
    ]),

    daq.Gauge(
        id="ANA",
        color={
            "gradient": True,
            "ranges": {"yellow": [0, 3], "red":[3, 4]}
        },
        label="Main Cock",
        labelPosition="bottom",
        min=0,
        max=4,
        size=200,
        # showCurrentValue=True,
        scale={"start": 0, "interval": 1, "labelInterval": 1},
        value=int(topic_msg[topic_prefix + "ANA/Level-Burner"])
    ),

    html.Br(),

    daq.Gauge(
        id="RND",
        color={
            "gradient": True,
            "ranges": {"yellow": [0, 3], "red":[3, 4]}
        },
        label={"label": "Random"},
        min=0,
        max=4,
        size=300,
        # step=1,
        # showCurrentValue=True,
        scale={"start": 0, "interval": 1, "labelInterval": 1},
        value=int(topic_msg[topic_prefix + "RND/Level-Burner"])
    ),

    html.Br(),

    dcc.Graph(
        id="graph_usage",
        # figure=fig_usage
    ),

    html.Hr(),

    html.H6(
        id="from_to"
    ),

    html.Div([
        dcc.RangeSlider(
            id='time-slider',
            min=0,
            max=dtime.days,
            value=[0, dtime.days], step=1,
            marks=slider_marks,
            included=True,
            allowCross=False),
    ], style={'width': '90%', 'padding': '5px 30px 10px 30px'}),


    html.Div([
        html.Div([
            dcc.Graph(id="graph_duration")
        ], className="six columns"),

        html.Div([
            dcc.Graph(id="graph_consumption")
        ], className="six columns"),
    ], className="row"),


    dcc.Interval(
        id="interval-component",
        interval=dash_web_page_update_interval,
        n_intervals=0
    ),

    html.Div(
        id="hidden-div",
        style={"display": "none"}
    )
])
#
# *** dash web page set up


@app.callback(
    [
        Output("from_to", "children"),
        Output("graph_duration", "figure"),
        Output("graph_consumption", "figure")
    ],
    [Input("time-slider", "value")]
)
def update_text(value):
    t_from = usage_hist_from + pd.Timedelta(value[0], unit="d")
    t_to = usage_hist_from + pd.Timedelta(value[1], unit="d")
    df_filter = (usage_hist_df["time"] >= t_from) & \
                (usage_hist_df["time"] <= t_to) & \
                (usage_hist_df["Indicator"] != 0)
    df = usage_hist_df[df_filter].groupby("Indicator").sum()
    df_duration_total = df["Duration_hr"].sum()
    df_consumption_total = df["Consumption_MJ"].sum()

    text_from_to = f'During the period from {t_from.strftime("%Y-%m-%d")} to {t_to.strftime("%Y-%m-%d")}'

    fig_duration = px.bar(
                    df,
                    y="Duration_hr",
                    title=f"Accumulated Working Hour = {df_duration_total:,.1f} hr",
                    # range_y=[df_duration_min, df_duration_max],
                    hover_data={"Duration_hr": ":,.1f"},
                    labels={
                            "Indicator": "Main Cock Position",
                            "Duration_hr": "Duration (hr)"

                    }
    )
    fig_duration.update_layout(xaxis_type="category")
    fig_duration.update_traces(marker_color="green")
    fig_consumption = px.bar(
                        df,
                        y="Consumption_MJ",
                        title=f"Accumulated Consumption = {df_consumption_total:,.0f}MJ",
                        labels={"Indicator": "Main Cock Position", "Consumption_MJ": "Consumption (MJ)"},
                        hover_data={"Consumption_MJ": ":,.0f"}
    )
    fig_consumption.update_layout(xaxis_type="category")

    return text_from_to, fig_duration, fig_consumption


@app.callback(
    [
        Output("LED1", "color"),
        Output("LED2", "color"),
        Output("LED3", "color"),
        Output("LED3_switch", "children"),
        Output("LED3_switch", "outline"),
        Output("ANA", "value"),
        Output("RND", "value"),
        Output("graph_usage", "figure"),
        Output("debug_txt","children")
    ],
    [Input("interval-component", "n_intervals")]
)
def update_indicator(n_intervals):

    client_MQTT.loop()

    if topic_msg[topic_prefix + "LED1/Status"] == "1":
        LED1_color = indicator_colors["on_r"]
    else:
        LED1_color = indicator_colors["off"]

    if topic_msg[topic_prefix + "LED2/Status"] == "1":
        LED2_color = indicator_colors["on_g"]
    else:
        LED2_color = indicator_colors["off"]

    if topic_msg[topic_prefix + "LED3/Status"] == "1":
        LED3_color = indicator_colors["on_y"]
        LED3_btn_lbl = "Turn Off"
        LED3_outline = False
    else:
        LED3_color = indicator_colors["off"]
        LED3_btn_lbl = "Turn On"
        LED3_outline = True

    ANA_Level_Burner = int(
        topic_msg[topic_prefix + "ANA/Level-Burner"])
    RND_Level_Burner = int(
        topic_msg[topic_prefix + "RND/Level-Burner"])

    global usage_hist_extract
    t_now = pd.Timestamp.now(tz=tz_hk).tz_localize(None)
    usage_hist_extract = usage_hist_extract.append(
                        {"time": t_now, "Indicator": RND_Level_Burner},
                        ignore_index=True)
    filter_showrange = (t_now - usage_hist_extract["time"]) > pd.Timedelta(usage_showrange)
    usage_hist_extract.drop(index=usage_hist_extract[filter_showrange].index, inplace=True)
    # print(usage_hist_extract["time"])

    fig_usage = px.line(
                        usage_hist_extract,
                        x=usage_hist_extract["time"],
                        y=usage_hist_extract["Indicator"],
                        title=f"Usage Pattern of last {usage_showrange}",
                        labels={"Indicator": "Main Cock Position"},
                        range_y=[0, 4]
    )

    debug_info = f"{broker_address}: {port}; Topic subscribe: {topic_subscribe}; Connect Code={connect_code}."

    return LED1_color, LED2_color, LED3_color, LED3_btn_lbl, LED3_outline, \
        ANA_Level_Burner, RND_Level_Burner, fig_usage, debug_info


@app.callback(
    Output("hidden-div", "children"),
    [Input("LED3_switch", "n_clicks"), ]
)
def click_button(n):
    if topic_msg[topic_prefix + "LED3/Status"] == "1":
        client_MQTT.publish(topic_publish, "0")
        return
    else:
        client_MQTT.publish(topic_publish, "1")
        return


def on_connect(client_MQTT, userdata, flags, rc):
    global connect_code
    connect_code = str(rc)
    print(f"Connected Code :{str(rc)}")
    # Subscribe Topic from here
    client_MQTT.subscribe(topic_subscribe)
    # print(f"Topic subscribed: {topic_subscribe}")


# Callback Function on Receiving the Subscribed Topic/Message
def on_message(client_MQTT, userdata, msg):
    topic_msg[msg.topic] = str(msg.payload, encoding="UTF-8")
    # print(f"{msg.topic} : {topic_msg[msg.topic]}")


client_MQTT = mqtt.Client()  # create new MQTT instance
# set username and password
client_MQTT.username_pw_set(user, password=password)
client_MQTT.on_connect = on_connect  # attach function to callback
client_MQTT.on_message = on_message  # attach function to callback
client_MQTT.connect(broker_address, port=port)  # connect to broker


if __name__ == "__main__":
    # client.loop_forever()
    # client_MQTT.loop_start()
    app.run_server(debug=True, dev_tools_hot_reload=False)
    # rc=0
    # while rc==0:
    #     rc=client_MQTT.loop()
    #     print(f"Inside loop rc: {rc}")
    # print(f"Outside loop rc: {rc}")

client_MQTT.loop_stop()
client_MQTT.disconnect()
