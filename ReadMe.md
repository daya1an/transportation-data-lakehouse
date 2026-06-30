```markdown
# Enterprise mobility data lakehouse

## What this project does

Think of a city's ride hailing service. Every trip, every rating, every pickup gets logged somewhere. This project takes all that messy raw data and turns it into clean, easy to read reports that city managers can actually use.

## What it can do

🚦 **Reads new data automatically** — no manual upload needed. New trip files get picked up on their own, even if something looks broken.

🔄 **Always shows the latest info** — if a trip detail changes, it updates the record instead of creating a duplicate.

✅ **Catches bad data early** — wrong ratings, broken dates, anything odd gets filtered out before it reaches anyone's dashboard.

📊 **Makes reports load fast** — instead of digging through raw transaction logs, reports pull from a simplified, ready to use version of the data.

🏙️ **Gives each city its own view** — Jaipur, Kochi, Surat and others each get their own private slice of the data, so local teams only see what matters to them.

## How the data flows

Raw files arrive and move through three clean up stages before reaching dashboards.

```
Raw trip files (S3)
        │
        ▼
┌───────────────────────────────┐
│          BRONZE LAYER           │
│  Save everything exactly as it  │
│  arrives. Nothing deleted.      │
└───────────────────────────────┘
        │
        ▼
┌───────────────────────────────┐
│          SILVER LAYER           │
│  Clean data, fix mistakes,      │
│  update changed records         │
└───────────────────────────────┘
        │
        ▼
┌───────────────────────────────┐
│           GOLD LAYER            │
│  Join everything into one big   │
│  ready to use table             │
└───────────────────────────────┘
        │
    ┌───┼───┐
    ▼   ▼   ▼
 Jaipur Kochi Surat
dashboard dashboard dashboard
```

🥉 **Bronze (raw stage)**
Everything gets saved exactly as it came in. Nothing is deleted or changed here. This is the safety net, the original copy.

🥈 **Silver (cleaned stage)**
Now the cleanup happens. Typos in column names get fixed, invalid ratings get removed, and if a trip record changes, the old version gets updated instead of duplicated. A calendar table (with holidays marked) also gets built here.

🥇 **Gold (ready stage)**
All the cleaned pieces get joined together into one easy to read table. Then it gets split city by city, so each region gets its own dashboard ready dataset.

## What it's built with

🔧 **PySpark** — does the heavy lifting of processing data
💾 **Delta Lake** — stores data safely and lets you look back at past versions
⚙️ **Databricks Delta Live Tables** — runs the whole pipeline automatically
☁️ **AWS S3** — where raw files are stored
📈 **Databricks SQL** — where reports and dashboards connect
```

Wrap the diagram block in triple backticks in your actual README so the alignment stays fixed (markdown renderers reflow plain text otherwise).