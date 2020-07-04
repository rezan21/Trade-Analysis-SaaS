print("-- EC2: importing process module")
from json import dumps as json_dumps
import http.client
from boto3 import client as boto3_client
from os import listdir
from pickle import dumps as pickle_dumps

from multiprocessing import Process, Manager # CPU Bound (AWS EC2 Scaling)
from threading import Thread # IO Bound parallelism (for AWS Lambda Calls)

from math import floor, ceil
from pandas import read_csv, to_datetime, DataFrame, options
options.mode.chained_assignment = None # Disable SettingWithCopyWarning
from plotly.utils import PlotlyJSONEncoder 
import plotly.graph_objs as go
from random import gauss


LAMBDA_API = "YOUR_API.execute-api.us-east-1.amazonaws.com"
LAMBDA_PATH = "/default/calculate_var"
S3_CLIENT = boto3_client('s3')
BUCKET_NAME = "YOUR-BUCKET-NAME"

UNITS = 1000 # stock units
counter = 0

def processData(stock, A):
    df = read_csv(f"./{stock}.csv", usecols=["Date","Adj Close"])

    df["Date"] = to_datetime(df["Date"], infer_datetime_format=True)
    df.sort_values("Date",ascending=True, inplace=True)

    df[f"sma"] = df["Adj Close"].rolling(A).mean()
    df["shifted_close"] = df["Adj Close"].shift(1)
    df["shifted_sma"] = df[f"sma"].shift(1)


    df.rename(columns={"Adj Close":"adj_close", "Date":"date" },inplace=True)

    df.reset_index(drop=True, inplace=True)
    return df

# trade strategy logic
def applyStrategy(df, V):
    global counter
    counter = 0
    def getSignal(x):
        global counter
        counter += 1
        if counter <= V:
            return("NO_ACTION")
        else:
            if ((x["shifted_close"] < x["shifted_sma"]) and (x["adj_close"] > x["sma"])):
                return("BUY")
            elif ((x["shifted_close"] > x["shifted_sma"]) and (x["adj_close"] < x["sma"])):
                return("SELL")
            else:
                return("NO_ACTION")
    df["signal"] = df.apply(getSignal,axis=1)
    return df


def getGenerators(df, V):
    # V-time window, mu and std
    signals = df[df["signal"] != "NO_ACTION"] # only buy/sell signals
    indecies = signals.index
    generators = [] # mu, std
    for index in indecies:
        v_window = df.iloc[index-V+1:index+1,:] # get V time windows
        v_window["return"] = v_window["adj_close"].pct_change() # returns (must be 1 less than V)

        mu = v_window["return"].mean()
        std = v_window["return"].std()
        generators.append((mu, std))
    signals["generators(mu, std)"] = generators
    return signals


def drawPlot(df, A, STOCK):
    df_buys = df[df["signal"]=="BUY"]
    df_sells = df[df["signal"]=="SELL"]
    df_all = df.loc[(df["signal"]=="BUY") | (df["signal"]=="SELL")]


    # plot timeseries, SMA, signals
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df['adj_close'],
        name="Adj Close",
    #     mode='lines',
    #     line_color='#257EDC',
    #     opacity=0.8
        )
    )
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df['sma'],
        name=f"SMA {A}",
        line_color='#c761ff',
        line=dict(width=2, dash="dot"),
        opacity=0.8
        )
    )
    fig.add_trace(go.Scatter(
        x=df_all["date"],
        y=df_all['adj_close'],
        name="In Position",
        mode='lines',
        line=dict(width=1, color="yellow"),
        opacity=0.7,
        )
    )
    fig.add_trace(go.Scatter(
        x=df_buys["date"],
        y=df_buys['adj_close'],
        name="BUY",
        mode='markers',
        opacity=1,
        marker_symbol="triangle-up",
        marker=dict(
            color='#39ff14',
            size=10
            )
        )
    )
    fig.add_trace(go.Scatter(
        x=df_sells["date"],
        y=df_sells['adj_close'],
        name="SELL",
        mode='markers',
        opacity=1,
        marker_symbol="triangle-down",
        marker=dict(
            color='#ff0800',
            size=10
            )
        )
    )

    fig.update_layout(
        title=f'{STOCK} Daily Chart',
        xaxis_title='Date',
        yaxis_title='Price ($)',
        template="plotly_dark",
    )


    graphJSON = json_dumps(fig, cls=PlotlyJSONEncoder)
    return graphJSON


def closeTrades(signals, df):
    def getPL(x):
        profit = (UNITS*x["Close_Price"]) - (UNITS*x["Open_Price"])

        if x["Signal"] == "SELL":
                profit = -profit
        return profit
    trade_signals = signals.drop(["sma","shifted_close","shifted_sma"],axis=1)
    # close price of a trade is open price(i.e: adj close) of next trade
    trade_signals["Close_Price"] = trade_signals["adj_close"].shift(-1)
    # similar for close/open date
    trade_signals["Close_Date"] = trade_signals["date"].shift(-1)

    # rename columns
    trade_signals = trade_signals.rename({"signal":"Signal", "date":"Open_Date", "adj_close":"Open_Price"}, axis=1)

    # Last trade(signal) will be closed with last data point
    lastInd = trade_signals.index[-1] # index of last trade
    trade_signals.at[lastInd,"Close_Price"] = df.iloc[-1]["adj_close"] # substitute last adj_close in all data with last trade's close_price
    trade_signals.at[lastInd,"Close_Date"] = df.iloc[-1]["date"]

    # get profit/loss
    trade_signals["P&L"] = trade_signals.apply(getPL, axis=1)

    # cummulative profit/lostt
    trade_signals["Cumulative_Profit"] = trade_signals["P&L"].cumsum()

    # reorder columns
    trade_signals = trade_signals[["Signal", "Open_Date", "Close_Date", "Open_Price", "Close_Price",
                                "P&L", "Cumulative_Profit", "generators(mu, std)"]]


    return(trade_signals)

def getVAR(dfrow, s, r, conf_lvls=[0.95, 0.99]):
    manager = Manager()
    shared_lists_dict = None
    shared_lists_dict = {f"cl_{cl}":manager.list() for cl in conf_lvls} # dictionary comprehension
  
    def run_in_par(n, dic):
        # generate random returns
        mu, std = dfrow["generators(mu, std)"]
        new_returns = [gauss(mu, std) for _ in range(n)]

        # sort returns accordingly
        if dfrow["Signal"] == "BUY":
            new_returns.sort(reverse=True)
        elif dfrow["Signal"] == "SELL":
            new_returns.sort()
        else:
            raise ValueError("signal is other than sell/buy")

        # calculate var
        for cl in conf_lvls:

            # the index of cl
            cl_indx = (len(new_returns) * cl) - 1 # -1 to encounter 0 indexing of the list

            # estimated return at that index
            cl_val = (new_returns[floor(cl_indx)] +  new_returns[ceil(cl_indx)]) / 2

            new_price = (1 + cl_val) * dfrow["Open_Price"]

            VaR = new_price * UNITS * dfrow["Open_Price"]
            dic[f"cl_{cl}"].append(VaR)

    processes = []
    for _ in range(r):

        p = Process(target=run_in_par, args=[int(s/r), shared_lists_dict])
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
    
    final_vars = {}
    for cl in conf_lvls:
        avg_var = 0
        for e in shared_lists_dict[f"cl_{cl}"]:
            avg_var += e
        avg_var = avg_var/r
        final_vars.update({f"cl_{cl}":avg_var})
        
    return list(final_vars.values())

def connect_lambda(api, path, json_, shared_list):
    c = http.client.HTTPSConnection(api)
    c.request("POST", path, json_)
    response = c.getresponse()
    data = [float(var) for var in response.read().decode().strip("[]").split(",")]
    shared_list.append(data)


def drawTable(df):
    tableDF = df

    DP = 4
    tableDF = tableDF.round(DP) # round to d.p.
    h_values = tableDF.columns.tolist()
    c_values = [tableDF[h] for h in h_values]
    h_values = [f"<b>{h}</b>" for h in h_values] # bold headers

    colours = ["#15a54c" if profit>0.0 else "#ee494f" for profit in tableDF["P&L"]] # colour condition
    colours = [colours for i in range(len(h_values))] # apply to each coloumn


    fig = go.Figure(data=[go.Table(
    header=dict(
        values=h_values,
        fill_color='#3b3b3b',
        line_color="black",
        align='left', font=dict(color='white', size=11)
    ),
    cells=dict(
        values=c_values,
        fill_color=colours,
        line_color="black",
        align='left', font=dict(color='white', size=9)
    ))
    ])
    fig.update_layout(
            title=f'Trade Positions:<br><span style="font-size:10px">*Rounded to {DP}d.p.</span><br><span style="font-size:10px">*scroll to see full trable.</span>',
            margin=dict(l=10, r=10, b=5, t=100, pad=0),

        )
    graphJSON = json_dumps(fig, cls=PlotlyJSONEncoder)
    return graphJSON

def drawSumTable(df):
    tableDF = df.reset_index(drop=False)

    DP = 3
    tableDF = tableDF.round(DP) # round to d.p.
    h_values = tableDF.columns.tolist()
    c_values = [tableDF[h] for h in h_values]
    h_values = [f"<b>{h}</b>" for h in h_values] # bold headers

    fig = go.Figure(data=[go.Table(
        header=dict(
            values=h_values,
            fill_color='#3b3b3b',
            line_color="black",
            align='left', font=dict(color='white', size=11)),
        cells=dict(
            values=c_values,
            line_color="black",
            align='left', font=dict(color='black', size=11))
        )
    ])
    fig.update_layout(
        title=f'Summary Table:<br><span style="font-size:10px">*Rounded to {DP}d.p.</span><br>',
        # autosize=False,
        # width=500,
        # height=500,
        margin=dict(l=10, r=10, b=5, t=100, pad=0),
        )
 
    graphJSON = json_dumps(fig, cls=PlotlyJSONEncoder)
    return graphJSON

# main function
def generate_results(STOCK, A, V, S, R, Res_Type):

    # AWS S3 - Download Required csv
    if f"{STOCK}.csv" not in listdir("."):
        S3_CLIENT.download_file(BUCKET_NAME, f"{STOCK}.csv", f'./{STOCK}.csv')

    df = processData(stock=STOCK, A=A)

    df = applyStrategy(df, V=V)

    signalsWithGen = getGenerators(df, V=V)

    closedTrades = closeTrades(signals=signalsWithGen, df=df)

    # VAR calculation
    if Res_Type == "ec2":
        closedTrades["vars"] = closedTrades.apply(getVAR, axis=1, args=(S,R)) # calculate VAR on ec2 using multi-processing
        closedTrades["VAR_95"] = closedTrades["vars"].apply(lambda x:x[0])
        closedTrades["VAR_99"] = closedTrades["vars"].apply(lambda x:x[1])
        table = closedTrades[["Signal", "Open_Date",  "Close_Date", "Open_Price", "Close_Price", "P&L", "Cumulative_Profit", "VAR_95", "VAR_99"]]


    elif Res_Type == "lambda":

        f_vars=[]
        for index, row in closedTrades.iterrows():
            temp_vars = []
            json_trade = json_dumps(
                {
                    "signal":row["Signal"],
                    "open_price":row["Open_Price"],
                    "mu":row["generators(mu, std)"][0],
                    "std":row["generators(mu, std)"][1],
                    "units":UNITS,
                    "n":int(S/R),
                }
            )
                
            threads = []
            for _ in range(R):
                t = Thread(target=connect_lambda, args=[LAMBDA_API, LAMBDA_PATH, json_trade, temp_vars])
                t.start()
                threads.append(t)

            for thread in threads:
                thread.join()
            
            # average values of multiple threads
            sum_v95, sum_v99 = 0, 0
            for tv in temp_vars:
                sum_v95 += tv[0]
                sum_v99 += tv[1]
            
            f_vars.extend([[sum_v95/R, sum_v99/R]])
            

        v95, v99 = [], []
        for fv in f_vars:
            v95.append(fv[0])
            v99.append(fv[1])

        closedTrades["VAR_95"] = v95
        closedTrades["VAR_99"] = v99
        table = closedTrades[["Signal", "Open_Date",  "Close_Date", "Open_Price", "Close_Price", "P&L", "Cumulative_Profit", "VAR_95", "VAR_99"]]
    
    # summary table
    sum_df = DataFrame(
    {
        "Total Trades":len(table),
        "Avg P&L":table["P&L"].mean(),
        "Total P&L":table["P&L"].sum(),
        "Avg. VAR 95":table["VAR_95"].mean(),
        "Avg. VAR 99":table["VAR_99"].mean(),
    }
    , index=["Value"]).T

    return pickle_dumps({
        "table":drawTable(table),
        "plot":drawPlot(df, A=A, STOCK=STOCK),
        "summary":drawSumTable(sum_df)
        })


print("-- EC2: end of process module")