import pandas as pd
import plotly.express as px
import streamlit as st
from data import available_years, load

st.set_page_config(page_title="Air quality in Poland", layout="wide")

POLLUTANTS = ["PM10", "PM2.5", "NO2", "SO2", "O3", "CO", "C6H6"]
WHO_ANNUAL = {"PM10": 15, "PM2.5": 5, "NO2": 10}
HAS_EU_LIMIT = {"PM10", "PM2.5", "NO2", "SO2", "C6H6"}

st.title("Air quality in Poland")
st.caption("GIOŚ measurements, processed with Spark + Delta Lake. Values in µg/m³.")

years = available_years()
if not years:
    st.warning("No gold data yet - run the gios_archive_backfill DAG first.")
    st.stop()

with st.sidebar:
    year = st.selectbox("Year", years[::-1])
    pollutant = st.selectbox("Pollutant", POLLUTANTS)

dim = load("dim_station")
annual = load("annual_compliance", filters=(("year", "=", year),))
annual = annual[(annual["pollutant"] == pollutant) & annual["is_representative"]].merge(
    dim, on="station_code", how="left"
)

tab_map, tab_trend, tab_weather, tab_dq = st.tabs(
    ["Stations", "Trends", "Smog & weather", "Data quality"]
)


def stations_tab(annual: pd.DataFrame) -> None:
    if annual.empty:
        st.info(f"No representative {pollutant} stations in {year}.")
        return
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stations", len(annual))
    c2.metric("Mean of annual means", f"{annual['annual_mean'].mean():.1f}")
    if pollutant in HAS_EU_LIMIT:
        c3.metric("EU compliant", f"{annual['eu_compliant'].mean() * 100:.0f}%")
    if pollutant in WHO_ANNUAL:
        c4.metric("Meets WHO 2021", f"{annual['meets_who_annual'].mean() * 100:.0f}%")

    fig = px.scatter_map(
        annual,
        lat="latitude",
        lon="longitude",
        color="annual_mean",
        size="annual_mean",
        size_max=14,
        hover_name="station_name",
        hover_data={"city": True, "annual_mean": ":.1f", "eu_exceedance_days": True},
        color_continuous_scale="YlOrRd",
        zoom=5.2,
        center={"lat": 52.0, "lon": 19.3},
        height=620,
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, width="stretch")

    if pollutant == "PM10":
        st.subheader("Most days above the EU daily limit (50 µg/m³, 35 days allowed)")
        worst = annual.sort_values("eu_exceedance_days", ascending=False).head(15)
        st.dataframe(
            worst[["station_name", "city", "voivodeship", "annual_mean", "eu_exceedance_days"]],
            hide_index=True,
            width="stretch",
        )


with tab_map:
    stations_tab(annual)

with tab_trend:
    monthly = load("monthly_city", columns=("city", "pollutant", "month", "mean"))
    monthly = monthly[monthly["pollutant"] == pollutant]
    biggest = ["Warszawa", "Kraków", "Wrocław", "Łódź", "Poznań", "Gdańsk", "Katowice"]
    cities = st.multiselect(
        "Cities",
        sorted(monthly["city"].dropna().unique()),
        default=[c for c in biggest if c in set(monthly["city"])][:4],
    )
    trend = monthly[monthly["city"].isin(cities)].sort_values("month")
    fig = px.line(trend, x="month", y="mean", color="city", markers=True, height=480)
    fig.update_layout(yaxis_title=f"{pollutant} monthly mean", xaxis_title=None)
    st.plotly_chart(fig, width="stretch")
    st.caption("Gaps between years only mean those years haven't been loaded.")

with tab_weather:
    pmw = load("daily_pm_weather", filters=(("year", "=", year),))
    if pmw.empty:
        st.info("No weather data for this year - run the weather_backfill DAG.")
    else:
        pmw["season"] = pmw["is_heating_season"].map({True: "heating", False: "non-heating"})
        c1, c2 = st.columns(2)
        with c1:
            sample = pmw.dropna(subset=["pm10"]).sample(min(len(pmw), 20000), random_state=1)
            fig = px.scatter(
                sample,
                x="temp_mean_c",
                y="pm10",
                color="season",
                opacity=0.3,
                labels={"temp_mean_c": "daily mean temperature [°C]", "pm10": "PM10"},
                height=420,
            )
            st.plotly_chart(fig, width="stretch")
        with c2:
            pmw["blh_bin"] = pd.cut(
                pmw["blh_mean_m"], bins=[0, 200, 400, 600, 800, 1000, 1500, 4000]
            )
            by_blh = pmw.groupby("blh_bin", observed=True)[["pm10", "pm25"]].mean().reset_index()
            by_blh["blh_bin"] = by_blh["blh_bin"].astype(str)
            fig = px.bar(
                by_blh.melt(id_vars="blh_bin", var_name="pollutant", value_name="mean"),
                x="blh_bin",
                y="mean",
                color="pollutant",
                barmode="group",
                labels={"blh_bin": "boundary layer height [m]"},
                height=420,
            )
            st.plotly_chart(fig, width="stretch")

        corr = pmw[["pm10", "temp_mean_c", "wind_mean_kmh", "blh_mean_m", "humidity_mean_pct"]]
        st.subheader("Correlation with daily PM10")
        st.dataframe(corr.corr()["pm10"].drop("pm10").round(2).to_frame().T, hide_index=True)

with tab_dq:
    dq = load("dq_results")
    if dq.empty:
        st.info("No quality runs yet.")
    else:
        latest = dq[dq["checked_at"] == dq["checked_at"].max()]
        st.caption(f"Last run: {latest['checked_at'].iloc[0]:%Y-%m-%d %H:%M} UTC")
        st.dataframe(
            latest[["table", "check", "severity", "passed", "failed_rows", "details"]],
            hide_index=True,
            width="stretch",
        )
