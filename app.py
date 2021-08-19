import argparse
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

def to_df(args):
    jstr = open(args.datafolder + args.dataset + ".json", "r").read()
    data = json.loads(jstr)

    rows = []
    stopwords = None

    # Read in list of stopwords if requested
    if args.stopwords is not None:
        print(args.stopwords)
        stopwords_str = open("stopwords/" + args.stopwords + ".json", "r").read()
        stopwords = set(json.loads(stopwords_str))

    # Create dataframe of messages
    for message in progressbar.progressbar(data["messages"]):
        if message["type"] == "message":
            text = message["text"]
            if type(text) == str:
                if args.lowercase:
                    text = text.lower()

                if stopwords is not None:
                    text = [wd for wd in text.split() if wd.lower() not in stopwords]
                    text = " ".join(text)

                sender = message["from"]
                date = read_date(message)

                row = [sender, text, date]
                rows.append(row)
    return pd.DataFrame(rows, columns=["sender", "text", "date"])

def n_grams(df, n=2, threshold=5):
    grams = {}

    for message in progressbar.progressbar(list(df["text"])):
        words = message.split()
        if len(words) >= n:
            for i in range(len(words)-n):
                n_gram = words[i:i+n]
                n_gram = " ".join(n_gram)
                grams[n_gram] = grams.get(n_gram, 0) + 1

    grams = [(n_gram, occ) for n_gram, occ in grams.items() if occ > threshold]
    grams = sorted(grams, key= lambda t: t[1])
    return grams

def main(args):
    df = to_df(args)
    print(df)

    grams = n_grams(df, n=3)
    for ngram, count in grams:
        print(ngram, count)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--datafolder", type=str, default="data/")
    parser.add_argument("--dataset", type=str, default="results")
    parser.add_argument("--stopwords", type=str, default=None)
    parser.add_argument("--lowercase", type=bool, default=False)
    args = parser.parse_args()

    main(args)