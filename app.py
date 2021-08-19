import datetime, json
import pandas as pd
import progressbar
from datetime import datetime as dt

def read_date(msg):
    dateformat = "%Y-%m-%dT%H:%M:%S"
    try:
       date = dt.strptime(msg["date"], dateformat)
    except TypeError:
        print("Pajari!")

    return date

def main():
    jstr = open("result.json", "r").read()
    data = json.loads(jstr)

    rows = []

    for message in progressbar.progressbar(data["messages"]):
        if message["type"] == "message":
            text = message["text"]
            sender = message["from"]
            date = read_date(message)

            row = [sender, text, date]
            rows.append(row)
    df = pd.DataFrame(rows, columns=["sender", "text", "date"])

    print(df)
if __name__ == '__main__':
    main()