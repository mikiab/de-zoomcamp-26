import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    import pandas as pd
    import plotly.express as px

    return dlt, pd, px


@app.cell
def _(dlt):
    # Attach to the existing pipeline instead of opening DuckDB directly
    pipeline = dlt.attach(pipeline_name="taxi_pipeline")

    # Get the readable dataset interface
    dataset = pipeline.dataset()
    return pipeline, dataset


@app.cell
def _(dataset, pd):
    # Use the dataset SQL interface to compute payment-type proportions
    relation = dataset(
        """
        SELECT
          payment_type,
          COUNT(*) AS trips,
          ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM taxi_data.taxi_trips
        GROUP BY payment_type
        ORDER BY trips DESC
        """
    )

    df = relation.df()  # pandas DataFrame
    return df, relation


@app.cell
def _(df, px):
    # Plot proportion by payment type, highlighting credit cards implicitly
    fig = px.bar(
        df,
        x="payment_type",
        y="pct",
        text="pct",
        labels={"payment_type": "Payment type", "pct": "Share of trips (%)"},
        title="Proportion of trips by payment type (Credit vs others)",
    )
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(
        yaxis_tickformat=".2f", uniformtext_minsize=8, uniformtext_mode="hide"
    )

    # Returning the figure directly allows marimo to render it in the UI
    return (fig,)


if __name__ == "__main__":
    app.run()
