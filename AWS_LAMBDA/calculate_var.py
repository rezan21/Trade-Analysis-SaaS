from math import floor, ceil
from random import gauss

def lambda_handler(event, context):
    conf_lvls=[0.95, 0.99]

    # generate random returns
    new_returns = [gauss(event["mu"], event["std"]) for _ in range(event["n"])]

    # sort returns accordingly
    if event["signal"] == "BUY":
        new_returns.sort(reverse=True)
    elif event["signal"] == "SELL":
        new_returns.sort()
    else:
        raise ValueError("signal is other than sell/buy")

    # calculate var
    final_vars = []
    for cl in conf_lvls:

        # the index of cl
        cl_indx = (len(new_returns) * cl) - 1 # -1 to encounter 0 indexing of the list

        # estimated return at that index
        cl_val = (new_returns[floor(cl_indx)] +  new_returns[ceil(cl_indx)]) / 2

        new_price = (1 + cl_val) * event["open_price"]

        VaR = new_price * event["units"] * event["open_price"]
        final_vars.append(VaR)

    return final_vars