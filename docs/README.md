# 📚 Documentation — Banking Modern Data Stack

Everything you need to understand, run, and explain this project.

## 🌐 Live (GitHub Pages)
- **📖 Full interactive guide:** https://satishs100kr-web.github.io/banking-modern-datastack/
- **🖥️ Interactive UI Tour:** https://satishs100kr-web.github.io/banking-modern-datastack/ui-tour.html

## 📂 Files in this folder

| File | What it is | Best for |
|---|---|---|
| **[PROJECT_GUIDE.md](PROJECT_GUIDE.md)** | The complete guide — 27 sections: architecture, every file line-by-line, internals, every UI screen, ports, exercises, troubleshooting, SCD2, dbt tests, 25-question mock interview, FAQ | deep reading |
| **[COMMANDS_JOURNEY.md](COMMANDS_JOURNEY.md)** | Every command + bug + fix, grouped by phase | quick command lookup |
| **[index.html](index.html)** | Interactive web guide — sidebar, search, copy buttons, 28 panels | browsing |
| **[ui-tour.html](ui-tour.html)** | Click-to-learn mockups of every tool's screens (27 screens) | learning the UIs |
| **[architecture.svg](architecture.svg)** | The pipeline diagram | the big picture |

## 🗺️ Suggested reading order (beginner → confident)
1. **PROJECT_GUIDE → §1–3** (What/Why, Architecture, Tech Stack) — the big picture.
2. **UI Tour** — open each tool and click the pins to see what you're working with.
3. **PROJECT_GUIDE → §5–10** (the 6 stages, line-by-line).
4. **§17 Exercises** — do them hands-on.
5. **§22–24** (Internals, Full Code, Mock Interview) — for mastery + interviews.
6. Keep **§18 Troubleshooting Playbook** open whenever something breaks.

---

*Pipeline: PostgreSQL → Debezium/Kafka (CDC) → MinIO → Airflow → Snowflake → dbt → Power BI* 🏦
