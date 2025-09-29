# /// script
# requires-python = "==3.12"
# dependencies = [
#     "altair==5.5.0",
#     "duckdb==1.4.0",
#     "emoji==2.15.0",
#     "marimo",
#     "pandas==2.3.2",
#     "plotly==6.3.0",
#     "polars==1.33.1",
#     "sqlglot==27.19.0",
#     "text-imp==0.2.0.1",
#     "vegafusion==2.0.2",
#     "vl-convert-python==1.8.0",
# ]
# ///

import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Marimo Lightning Talk""")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Why Marimo?

    - Very helpful UI components
    - Switch between dataframes and SQL easily
    - Deterministic cell execution order
    - Built in dependecy management
    - [Works in webassembly](https://docs.marimo.io/api/plotting/)
        - Make browser tools for non-engineers!
        - See live examples in the [docs](https://docs.marimo.io/api/inputs/form/)
    - Run as a script
    - Formatter and AI built in
    - Export to jupyter worst case
    """
    )
    return


@app.cell
def _():
    import text_imp
    import polars as pl
    import marimo as mo
    from datetime import datetime, timedelta
    import altair as alt
    import emoji
    import plotly.express as px

    _temp = alt.data_transformers.enable("vegafusion")
    return alt, datetime, emoji, mo, pl, px, text_imp, timedelta


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Text-Imp: play with your iMessage Data

    Your texts are stored in a SQLite database on your Mac. 

    Why not play with it?
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## View your most recently added contacts""")
    return


@app.cell
def _(pl, text_imp):
    def redact_keep_first(s):
        if s is None:
            return None
        s = str(s)
        if len(s) <= 1:
            return s
        return s[0] + "*" * (len(s) - 1)

    contacts = text_imp.get_contacts().with_columns(
        pl.col("first_name")
        .map_elements(redact_keep_first, return_dtype=pl.Utf8)
        .alias("first_name"),
        pl.col("last_name")
        .map_elements(redact_keep_first, return_dtype=pl.Utf8)
        .alias("last_name"),
    )
    most_recent_contacts = contacts.sort("creation_date", descending=True)
    most_recent_contacts
    return contacts, redact_keep_first


@app.cell
def _(contacts, pl, redact_keep_first, text_imp):
    chat_handles = text_imp.get_chats()
    messages = text_imp.get_messages()
    chats = text_imp.get_chats()

    combined = (
        messages.join(chats, left_on="chat_id", right_on="rowid", how="inner")
        .join(contacts, left_on="name", right_on="normalized_contact_id", how="inner")
        .with_columns(
            pl.col("text")
            .map_elements(redact_keep_first, return_dtype=pl.Utf8)
            .alias("text"),
        )
    )
    return combined, messages


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## View your message data""")
    return


@app.cell
def _(combined):
    combined
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Do quick transformations without code""")
    return


@app.cell
def _(combined, mo):
    mo.ui.dataframe(combined)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## What time of day do I text the most?""")
    return


@app.cell(hide_code=True)
def _(alt, combined):
    messages_by_hour = (
        alt.Chart(combined)
        .mark_point()
        .encode(
            x=alt.X(field="date", type="temporal", timeUnit="hours"),
            y=alt.Y(aggregate="count", type="quantitative"),
            tooltip=[
                alt.Tooltip(field="date", timeUnit="hours", title="date"),
                alt.Tooltip(aggregate="count"),
            ],
        )
        .properties(height=290, width="container", config={"axis": {"grid": True}})
    )
    messages_by_hour
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## What Emojis do I spam?""")
    return


@app.cell
def _(datetime, emoji, messages, pl, px, timedelta):
    def extract_emojis(text: str) -> list[str]:
        return [e["emoji"] for e in emoji.emoji_list(text)]

    one_week_ago = datetime.now() - timedelta(days=365)

    emoji_df = messages.filter(pl.col("date") >= pl.lit(one_week_ago)).with_columns(
        pl.col("text")
        .map_elements(extract_emojis, return_dtype=pl.List(pl.Utf8))
        .alias("emojis")
    )

    df_exploded = emoji_df.explode("emojis")

    emoji_counts = (
        df_exploded.filter(pl.col("emojis").is_not_null() & pl.col("is_from_me"))
        .group_by("emojis")
        .len()
        .rename({"len": "emoji_count"})
        .sort("emoji_count", descending=True)
    )

    px.bar(
        emoji_counts.to_pandas().head(10),
        x="emojis",
        y="emoji_count",
        title="My Top Used Emojis",
        labels={"emojis": "Emoji", "emoji_count": "Count"},
        height=400,
        width=600,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Widgets""")
    return


@app.cell
def _(mo):
    form = (
        mo.md('''
        **Phrase Search**

        {phrase}

        {start_date}
    
        {end_date}

    ''')
        .batch(
            phrase=mo.ui.text(label="phrase", value="korea"),
            start_date=mo.ui.date(label="start date", value="2022-01-01"),
            end_date=mo.ui.date(label="end date", value="2025-01-01"),
        )
        .form(show_clear_button=True, bordered=False)
    )
    form
    return (form,)


@app.cell
def _(form, messages, pl):
    messages.filter(
        (pl.col("date") >= pl.lit(form.value["start_date"]))
        & (pl.col("date") <= pl.lit(form.value["end_date"]))
        & pl.col("text")
            .str.to_lowercase()
            .str.contains(form.value["phrase"].lower(), literal=True)
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Switch Between SQL and Dataframes """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Contacts who I haven't messaged in a while""")
    return


@app.cell
def _(combined, mo):
    _df = mo.sql(
        f"""
        SELECT 
            first_name, 
            last_name, 
            MAX(date) AS latest_date,
            count(*) AS message_count
        FROM combined
        GROUP BY first_name, last_name
        ORDER BY latest_date asc;
        """
    )
    return


@app.cell
def _(alt, sql_out):
    _chart = (
        alt.Chart(sql_out)
        .mark_bar()
        .encode(
            x=alt.X(field='latest_date', type='temporal', timeUnit='day'),
            y=alt.Y(aggregate='count', type='quantitative'),
            tooltip=[
                alt.Tooltip(field='latest_date', timeUnit='day', title='latest_date'),
                alt.Tooltip(aggregate='count')
            ]
        )
        .properties(
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': False
                }
            }
        )
    )
    _chart
    return


if __name__ == "__main__":
    app.run()
